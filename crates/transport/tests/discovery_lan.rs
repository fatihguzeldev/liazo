use liazo_transport::{
    Identity,
    discovery::{Advertisement, discover},
};
use std::time::Duration;

#[tokio::test]
#[ignore = "requires local multicast networking"]
async fn a_live_advertisement_is_discovered_as_an_untrusted_route_hint() {
    let identity = Identity::generate().unwrap();
    let _advertisement = Advertisement::start(identity.fingerprint(), 45810).unwrap();
    let providers = discover(Duration::from_secs(3)).await.unwrap();
    assert!(providers.iter().any(|provider| {
        provider.advertised_fingerprint == identity.fingerprint().to_string()
            && provider
                .addresses
                .iter()
                .any(|address| address.port() == 45810)
    }));
}
