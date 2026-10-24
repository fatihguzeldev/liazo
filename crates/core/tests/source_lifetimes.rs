use liazo_core::{
    catalog::{SourceDescriptor, SourceRegistry, SourceResolutionError},
    protocol::{SourceAvailability, SourceKind},
};

fn window(instance: &str) -> SourceDescriptor {
    SourceDescriptor {
        id: "window:1".into(),
        instance: instance.into(),
        label: "window".into(),
        kind: SourceKind::Window,
        availability: SourceAvailability::Available,
        pixel_width: Some(1200),
        pixel_height: Some(800),
        scale: Some(2.0),
        process_id: Some(42),
    }
}

#[test]
fn metadata_changes_preserve_identity_but_recycled_native_handles_do_not() {
    let mut registry = SourceRegistry::default();
    let first = registry
        .replace_kind(SourceKind::Window, vec![window("pid42:birth1")])
        .unwrap()
        .changed
        .remove(0)
        .key;
    let mut changed = window("pid42:birth1");
    changed.pixel_width = Some(1400);
    changed.label = "edited title".into();
    let update = registry
        .replace_kind(SourceKind::Window, vec![changed])
        .unwrap();
    assert_eq!(update.changed[0].key, first);
    let replacement = registry
        .replace_kind(SourceKind::Window, vec![window("pid42:birth2")])
        .unwrap();
    assert_eq!(replacement.removed, vec![first.clone()]);
    assert_ne!(replacement.changed[0].key, first);
    assert!(matches!(
        registry.resolve(&first),
        Err(SourceResolutionError::StaleGeneration)
    ));
}

#[test]
fn disappearing_and_reappearing_sources_get_a_new_generation() {
    let mut registry = SourceRegistry::default();
    let first = registry
        .replace_kind(SourceKind::Window, vec![window("one")])
        .unwrap()
        .changed
        .remove(0)
        .key;
    assert_eq!(
        registry
            .replace_kind(SourceKind::Window, vec![])
            .unwrap()
            .removed,
        vec![first.clone()]
    );
    assert!(matches!(
        registry.resolve(&first),
        Err(SourceResolutionError::Missing)
    ));
    let returned = registry
        .replace_kind(SourceKind::Window, vec![window("one")])
        .unwrap();
    assert_ne!(returned.changed[0].key, first);
}

#[test]
fn permission_failures_preserve_identity_and_cannot_resolve_as_available() {
    let mut registry = SourceRegistry::default();
    let key = registry
        .replace_kind(SourceKind::Window, vec![window("one")])
        .unwrap()
        .changed
        .remove(0)
        .key;
    registry
        .mark_unavailable(SourceKind::Window, SourceAvailability::PermissionRequired)
        .unwrap();
    assert!(matches!(
        registry.resolve(&key),
        Err(SourceResolutionError::PermissionRequired)
    ));
    registry
        .replace_kind(SourceKind::Window, vec![window("one")])
        .unwrap();
    assert_eq!(registry.resolve(&key).unwrap().key, key);
}

#[test]
fn invalid_snapshots_do_not_partially_replace_a_catalog() {
    let mut registry = SourceRegistry::default();
    let key = registry
        .replace_kind(SourceKind::Window, vec![window("one")])
        .unwrap()
        .changed
        .remove(0)
        .key;
    let revision = registry.revision();
    assert!(
        registry
            .replace_kind(SourceKind::Window, vec![window("one"), window("two")])
            .is_err()
    );
    assert_eq!(registry.revision(), revision);
    assert!(registry.resolve(&key).is_ok());
}
