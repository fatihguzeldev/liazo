use std::{
    ffi::OsStr,
    fs::OpenOptions,
    io,
    marker::PhantomData,
    os::windows::{
        ffi::OsStrExt,
        io::{AsRawHandle, FromRawHandle, OwnedHandle},
        process::ExitStatusExt,
    },
    path::Path,
    process::ExitStatus,
    ptr::{null, null_mut},
};
use windows_sys::Win32::{
    Foundation::{
        DUPLICATE_SAME_ACCESS, DuplicateHandle, HANDLE, WAIT_FAILED, WAIT_OBJECT_0, WAIT_TIMEOUT,
    },
    System::Threading::{
        CREATE_NO_WINDOW, CreateProcessW, DeleteProcThreadAttributeList,
        EXTENDED_STARTUPINFO_PRESENT, GetCurrentProcess, GetExitCodeProcess, INFINITE,
        InitializeProcThreadAttributeList, LPPROC_THREAD_ATTRIBUTE_LIST,
        PROC_THREAD_ATTRIBUTE_HANDLE_LIST, PROCESS_INFORMATION, STARTF_USESTDHANDLES,
        STARTUPINFOEXW, TerminateProcess, UpdateProcThreadAttribute, WaitForSingleObject,
    },
};

pub(super) fn spawn_host(executable: &Path, args: &[&OsStr]) -> io::Result<HostChild> {
    let program = wide(executable.as_os_str())?;
    if program.is_empty() || program.contains(&(b'"' as u16)) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid executable path",
        ));
    }
    let mut command_line = vec![b'"' as u16];
    command_line.extend_from_slice(&program);
    command_line.push(b'"' as u16);
    for arg in args {
        command_line.push(b' ' as u16);
        quote_argument(&wide(arg)?, &mut command_line);
    }
    command_line.push(0);
    if command_line.len() > 32_767 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "command line is too long",
        ));
    }
    let mut program = program;
    program.push(0);

    let null_file = OpenOptions::new().read(true).write(true).open(r"\\.\NUL")?;
    let mut null_handle = null_mut();
    // SAFETY: The source file and current-process pseudo handles are valid. The
    // duplicate is a new, inheritable handle owned by this function alone.
    if unsafe {
        DuplicateHandle(
            GetCurrentProcess(),
            null_file.as_raw_handle(),
            GetCurrentProcess(),
            &mut null_handle,
            0,
            1,
            DUPLICATE_SAME_ACCESS,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: DuplicateHandle succeeded and ownership has not been transferred.
    let null_handle = unsafe { OwnedHandle::from_raw_handle(null_handle) };
    let handles = [null_handle.as_raw_handle()];
    let attributes = AttributeList::new(&handles)?;
    let mut startup = STARTUPINFOEXW::default();
    startup.StartupInfo.cb = size_of::<STARTUPINFOEXW>() as u32;
    startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
    startup.StartupInfo.hStdInput = handles[0];
    startup.StartupInfo.hStdOutput = handles[0];
    startup.StartupInfo.hStdError = handles[0];
    startup.lpAttributeList = attributes.as_ptr();
    let mut process = PROCESS_INFORMATION::default();
    // SAFETY: Both strings are NUL-terminated; command_line is writable. All
    // startup data and the single allowed NUL handle outlive CreateProcessW.
    // HANDLE_LIST excludes every other inheritable handle, including MCP pipes.
    if unsafe {
        CreateProcessW(
            program.as_ptr(),
            command_line.as_mut_ptr(),
            null(),
            null(),
            1,
            CREATE_NO_WINDOW | EXTENDED_STARTUPINFO_PRESENT,
            null(),
            null(),
            &startup.StartupInfo,
            &mut process,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: Successful CreateProcessW returns two newly owned, valid handles.
    // Closing the thread handle does not stop the process or its primary thread.
    let (handle, thread) = unsafe {
        (
            OwnedHandle::from_raw_handle(process.hProcess),
            OwnedHandle::from_raw_handle(process.hThread),
        )
    };
    drop(thread);
    Ok(HostChild {
        handle,
        #[cfg(test)]
        id: process.dwProcessId,
    })
}

fn wide(value: &OsStr) -> io::Result<Vec<u16>> {
    let value: Vec<_> = value.encode_wide().collect();
    if value.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "argument contains NUL",
        ));
    }
    Ok(value)
}

fn quote_argument(arg: &[u16], command_line: &mut Vec<u16>) {
    const QUOTE: u16 = b'"' as u16;
    const BACKSLASH: u16 = b'\\' as u16;
    command_line.push(QUOTE);
    let mut backslashes = 0;
    for &unit in arg {
        if unit == BACKSLASH {
            backslashes += 1;
            continue;
        }
        command_line.extend(std::iter::repeat_n(BACKSLASH, backslashes));
        if unit == QUOTE {
            command_line.extend(std::iter::repeat_n(BACKSLASH, backslashes + 1));
        }
        command_line.push(unit);
        backslashes = 0;
    }
    command_line.extend(std::iter::repeat_n(BACKSLASH, backslashes * 2));
    command_line.push(QUOTE);
}

struct AttributeList<'a> {
    storage: Box<[usize]>,
    _handles: PhantomData<&'a [HANDLE]>,
}

impl<'a> AttributeList<'a> {
    fn new(handles: &'a [HANDLE]) -> io::Result<Self> {
        let mut bytes = 0;
        // SAFETY: The first call only obtains the required allocation size.
        unsafe { InitializeProcThreadAttributeList(null_mut(), 1, 0, &mut bytes) };
        if bytes == 0 {
            return Err(io::Error::last_os_error());
        }
        let mut storage = vec![0usize; bytes.div_ceil(size_of::<usize>())].into_boxed_slice();
        let pointer = storage.as_mut_ptr().cast();
        // SAFETY: The allocation is pointer-aligned and at least bytes long.
        if unsafe { InitializeProcThreadAttributeList(pointer, 1, 0, &mut bytes) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let list = Self {
            storage,
            _handles: PhantomData,
        };
        // SAFETY: The initialized list has one slot. The borrowed handle array
        // remains valid until this list is deleted; only real NUL handles enter it.
        if unsafe {
            UpdateProcThreadAttribute(
                list.as_ptr(),
                0,
                PROC_THREAD_ATTRIBUTE_HANDLE_LIST as usize,
                handles.as_ptr().cast(),
                size_of_val(handles),
                null_mut(),
                null(),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(list)
    }

    fn as_ptr(&self) -> LPPROC_THREAD_ATTRIBUTE_LIST {
        self.storage.as_ptr().cast_mut().cast()
    }
}

impl Drop for AttributeList<'_> {
    fn drop(&mut self) {
        // SAFETY: Only successfully initialized attribute lists construct Self.
        unsafe { DeleteProcThreadAttributeList(self.as_ptr()) };
    }
}

pub(super) struct HostChild {
    handle: OwnedHandle,
    #[cfg(test)]
    id: u32,
}

impl HostChild {
    #[cfg(test)]
    pub(super) fn id(&self) -> u32 {
        self.id
    }

    pub(super) fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.status(0)
    }

    pub(super) fn wait(&mut self) -> io::Result<ExitStatus> {
        self.status(INFINITE)?
            .ok_or_else(|| io::Error::other("process wait timed out"))
    }

    pub(super) fn kill(&mut self) -> io::Result<()> {
        // SAFETY: This handle refers only to the process created by spawn_host.
        if unsafe { TerminateProcess(self.handle.as_raw_handle(), 1) } != 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if self.try_wait()?.is_some() {
            Ok(())
        } else {
            Err(error)
        }
    }

    fn status(&self, timeout: u32) -> io::Result<Option<ExitStatus>> {
        // SAFETY: The process handle remains owned and open throughout this call.
        match unsafe { WaitForSingleObject(self.handle.as_raw_handle(), timeout) } {
            WAIT_TIMEOUT => Ok(None),
            WAIT_OBJECT_0 => {
                let mut code = 0;
                // SAFETY: code is writable and the waited process handle is valid.
                if unsafe { GetExitCodeProcess(self.handle.as_raw_handle(), &mut code) } == 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(Some(ExitStatus::from_raw(code)))
            }
            WAIT_FAILED => Err(io::Error::last_os_error()),
            _ => Err(io::Error::other("unexpected process wait result")),
        }
    }
}
