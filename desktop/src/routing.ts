const addressPrefix = "liazo.address.";
const lastProviderKey = "liazo.last-provider";
const memory = new Map<string, string>();

function read(key: string): string {
  const cached = memory.get(key);
  if (cached !== undefined) return cached;
  try {
    return localStorage.getItem(key) ?? "";
  } catch {
    return "";
  }
}

function write(key: string, value: string) {
  memory.set(key, value);
  try {
    localStorage.setItem(key, value);
  } catch {
    // The current window can still remember routes when storage is unavailable.
  }
}

function remove(key: string) {
  memory.set(key, "");
  try {
    localStorage.removeItem(key);
  } catch {
    // Routing hints never determine whether a computer is trusted.
  }
}

function validAddress(address: string): boolean {
  if (address.length > 512) return false;
  const match = /^(?:\[[^\]\s]+\]|[^:\s]+):(\d{1,5})$/.exec(address);
  return !!match && Number(match[1]) > 0 && Number(match[1]) <= 65535;
}

export function rememberedAddress(provider: string): string {
  const address = read(addressPrefix + provider);
  return validAddress(address) ? address : "";
}

export function rememberedProvider(): string {
  return read(lastProviderKey);
}

export function rememberAddress(provider: string, address: string) {
  if (!provider || !validAddress(address)) return;
  write(addressPrefix + provider, address);
  write(lastProviderKey, provider);
}

export function forgetAddress(provider: string) {
  remove(addressPrefix + provider);
  if (rememberedProvider() === provider) remove(lastProviderKey);
}
