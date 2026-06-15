use std::fs;
use std::path::{Path, PathBuf};

use rcgen::{
    CertificateParams, DistinguishedName, DnType, DnValue, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};

const FIXTURE_DIR: &str = "src/simulation/protocol/.generated";

struct GeneratedCa {
    cert_pem: String,
    key_pem: String,
    issuer: Issuer<'static, KeyPair>,
}

struct GeneratedServer {
    cert_pem: String,
    key_pem: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let fixture_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?).join(FIXTURE_DIR);
    fs::create_dir_all(&fixture_dir)?;

    let mediated_ca = generate_ca("agentdp TLS CA")?;
    let upstream_ca = generate_ca("agentdp fixed simulation upstream CA")?;
    let upstream_server = generate_server(
        &upstream_ca.issuer,
        "allowed.test",
        ["allowed.test", "blocked.test", "bypass.test"],
    )?;

    write_fixture_readme(&fixture_dir)?;
    write_fixture(&fixture_dir, "mediated-ca.cert.pem", &mediated_ca.cert_pem)?;
    write_fixture(&fixture_dir, "mediated-ca.key.pem", &mediated_ca.key_pem)?;
    write_fixture(&fixture_dir, "upstream-ca.cert.pem", &upstream_ca.cert_pem)?;
    write_fixture(&fixture_dir, "upstream-server.cert.pem", &upstream_server.cert_pem)?;
    write_fixture(&fixture_dir, "upstream-server.key.pem", &upstream_server.key_pem)?;

    Ok(())
}

fn generate_ca(common_name: &str) -> Result<GeneratedCa, rcgen::Error> {
    let params = ca_params(common_name);
    let key = KeyPair::generate()?;
    let cert_pem = params.self_signed(&key)?.pem();
    let key_pem = key.serialize_pem();
    let issuer = Issuer::new(params, key);

    Ok(GeneratedCa {
        cert_pem,
        key_pem,
        issuer,
    })
}

fn generate_server(
    issuer: &Issuer<'_, impl rcgen::SigningKey>,
    common_name: &str,
    subject_alt_names: impl IntoIterator<Item = &'static str>,
) -> Result<GeneratedServer, rcgen::Error> {
    let mut params = CertificateParams::new(subject_alt_names.into_iter().map(str::to_owned).collect::<Vec<_>>())?;
    params.distinguished_name = distinguished_name(common_name);
    params.is_ca = IsCa::ExplicitNoCa;
    params.use_authority_key_identifier_extension = true;
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature, KeyUsagePurpose::KeyEncipherment];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];

    let key = KeyPair::generate()?;
    let cert_pem = params.signed_by(&key, issuer)?.pem();
    let key_pem = key.serialize_pem();

    Ok(GeneratedServer { cert_pem, key_pem })
}

fn ca_params(common_name: &str) -> CertificateParams {
    let mut params = CertificateParams::default();
    params.distinguished_name = distinguished_name(common_name);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    params.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params
}

fn distinguished_name(common_name: &str) -> DistinguishedName {
    let mut name = DistinguishedName::new();
    name.push(DnType::CommonName, DnValue::Utf8String(common_name.to_owned()));
    name
}

fn write_fixture_readme(fixture_dir: &Path) -> std::io::Result<()> {
    write_fixture(
        fixture_dir,
        "README.md",
        "Generated TLS fixtures for network simulation tests.\n\
         \n\
         These PEM files are test-only certificate authorities and server identities for\n\
         deterministic local TLS scenarios. They are not runtime secrets.\n\
         \n\
         Regenerate with:\n\
         \n\
         ```sh\n\
         cargo run -p agentdp-network-tests --bin generate-network-test-tls-fixtures\n\
         ```\n",
    )
}

fn write_fixture(fixture_dir: &Path, name: &str, contents: &str) -> std::io::Result<()> {
    fs::write(fixture_dir.join(name), contents)
}
