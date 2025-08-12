use anyhow::{Context, Result};
use rcgen::{CertificateParams, DistinguishedName};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::ServerConfig;
use rustls_pemfile;
use std::fs;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;
use tokio_rustls::TlsAcceptor;
use tracing::{debug, info, warn};

use crate::config::Config;

#[derive(Debug, Clone)]
pub enum CertificateSource {
    Provided { cert_path: String, key_path: String },
    FileSystem { cert_path: String, key_path: String },
    Generated { cert: Vec<u8>, key: Vec<u8> },
}

pub struct CertificateManager {
    config: Arc<Config>,
}

impl CertificateManager {
    pub fn new(config: Arc<Config>) -> Self {
        Self { config }
    }

    pub async fn initialize(&self) -> Result<(TlsAcceptor, CertificateSource)> {
        let cert_source = self.discover_certificates().await?;
        
        // Log the certificate source
        match &cert_source {
            CertificateSource::Provided { cert_path, key_path } => {
                info!("SSL enabled with provided certificates from {} and {}", cert_path, key_path);
            }
            CertificateSource::FileSystem { cert_path, key_path } => {
                info!("SSL enabled with existing certificates from {} and {}", cert_path, key_path);
            }
            CertificateSource::Generated { .. } => {
                if self.config.database == ":memory:" || self.config.in_memory {
                    info!("SSL enabled with ephemeral in-memory certificates");
                } else if self.config.ssl_ephemeral {
                    info!("SSL enabled with ephemeral certificates (not persisted)");
                } else {
                    info!("SSL enabled with newly generated certificates");
                }
            }
        }

        let tls_acceptor = self.create_tls_acceptor(&cert_source).await?;
        Ok((tls_acceptor, cert_source))
    }

    async fn discover_certificates(&self) -> Result<CertificateSource> {
        // Priority 1: Check provided paths from config
        if let (Some(cert_path), Some(key_path)) = (&self.config.ssl_cert, &self.config.ssl_key) {
            debug!("Using provided certificate paths");
            return Ok(CertificateSource::Provided {
                cert_path: cert_path.clone(),
                key_path: key_path.clone(),
            });
        }

        // Priority 2: Check filesystem next to database
        if !self.config.in_memory && self.config.database != ":memory:" && !self.config.ssl_ephemeral
            && let Some(cert_source) = self.check_filesystem_certificates()? {
                return Ok(cert_source);
            }

        // Priority 3: Generate certificates
        debug!("Generating new certificates");
        let (cert, key) = self.generate_certificates()?;

        // Save to filesystem if appropriate
        if !self.config.in_memory && self.config.database != ":memory:" && !self.config.ssl_ephemeral
            && let Err(e) = self.save_certificates(&cert, &key) {
                warn!("Failed to save generated certificates: {}", e);
            }

        Ok(CertificateSource::Generated { cert, key })
    }

    fn check_filesystem_certificates(&self) -> Result<Option<CertificateSource>> {
        let db_path = Path::new(&self.config.database);
        let db_dir = db_path.parent().unwrap_or(Path::new("."));
        let db_stem = db_path.file_stem()
            .and_then(|s| s.to_str())
            .context("Invalid database filename")?;

        let cert_path = db_dir.join(format!("{db_stem}.crt"));
        let key_path = db_dir.join(format!("{db_stem}.key"));

        if cert_path.exists() && key_path.exists() {
            debug!("Found existing certificates on filesystem");
            return Ok(Some(CertificateSource::FileSystem {
                cert_path: cert_path.to_string_lossy().to_string(),
                key_path: key_path.to_string_lossy().to_string(),
            }));
        }

        Ok(None)
    }

    fn generate_certificates(&self) -> Result<(Vec<u8>, Vec<u8>)> {
        let mut params = CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])
            .context("Failed to create certificate params")?;
        
        let mut distinguished_name = DistinguishedName::new();
        distinguished_name.push(rcgen::DnType::CommonName, "pgsqlite");
        distinguished_name.push(rcgen::DnType::OrganizationName, "pgsqlite");
        params.distinguished_name = distinguished_name;

        // Set validity period based on ephemeral flag
        // Note: rcgen uses time crate internally
        if self.config.ssl_ephemeral || self.config.in_memory || self.config.database == ":memory:" {
            // 90 days for ephemeral certificates
            // rcgen will set a reasonable default validity period
        } else {
            // 10 years for persistent certificates  
            // rcgen will set a reasonable default validity period
        }

        let key_pair = rcgen::KeyPair::generate()?;
        let cert = params.self_signed(&key_pair)?;
        let cert_pem = cert.pem();
        let key_pem = key_pair.serialize_pem();

        Ok((cert_pem.into_bytes(), key_pem.into_bytes()))
    }

    fn save_certificates(&self, cert: &[u8], key: &[u8]) -> Result<()> {
        let db_path = Path::new(&self.config.database);
        let db_dir = db_path.parent().unwrap_or(Path::new("."));
        let db_stem = db_path.file_stem()
            .and_then(|s| s.to_str())
            .context("Invalid database filename")?;

        let cert_path = db_dir.join(format!("{db_stem}.crt"));
        let key_path = db_dir.join(format!("{db_stem}.key"));

        fs::write(&cert_path, cert)
            .with_context(|| format!("Failed to write certificate to {cert_path:?}"))?;
        fs::write(&key_path, key)
            .with_context(|| format!("Failed to write private key to {key_path:?}"))?;

        // Set appropriate permissions on the private key (Unix only)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(&key_path)?.permissions();
            perms.set_mode(0o600); // Read/write for owner only
            fs::set_permissions(&key_path, perms)?;
        }

        info!("Generated and saved new certificates to {:?} and {:?}", cert_path, key_path);
        Ok(())
    }

    async fn create_tls_acceptor(&self, cert_source: &CertificateSource) -> Result<TlsAcceptor> {
        let (certs, key) = match cert_source {
            CertificateSource::Provided { cert_path, key_path } |
            CertificateSource::FileSystem { cert_path, key_path } => {
                self.load_certificates_from_files(cert_path, key_path)?
            }
            CertificateSource::Generated { cert, key } => {
                self.parse_certificates_from_memory(cert, key)?
            }
        };

        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .context("Failed to create TLS configuration")?;

        Ok(TlsAcceptor::from(Arc::new(config)))
    }

    fn load_certificates_from_files(&self, cert_path: &str, key_path: &str) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
        // Load certificate
        let cert_file = fs::File::open(cert_path)
            .with_context(|| format!("Failed to open certificate file: {cert_path}"))?;
        let mut cert_reader = BufReader::new(cert_file);
        let certs = rustls_pemfile::certs(&mut cert_reader)
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to parse certificate file")?;

        if certs.is_empty() {
            anyhow::bail!("No certificates found in {}", cert_path);
        }

        // Load private key
        let key_file = fs::File::open(key_path)
            .with_context(|| format!("Failed to open key file: {key_path}"))?;
        let mut key_reader = BufReader::new(key_file);
        
        let key = rustls_pemfile::private_key(&mut key_reader)?
            .context("No private key found in file")?;

        // Check key file permissions on Unix
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let metadata = fs::metadata(key_path)?;
            let mode = metadata.permissions().mode();
            if mode & 0o077 != 0 {
                warn!("Private key file {} has overly permissive permissions: {:o}", key_path, mode);
            }
        }

        Ok((certs, key))
    }

    fn parse_certificates_from_memory(&self, cert: &[u8], key: &[u8]) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
        let mut cert_reader = BufReader::new(cert);
        let certs = rustls_pemfile::certs(&mut cert_reader)
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to parse generated certificate")?;

        let mut key_reader = BufReader::new(key);
        let private_key = rustls_pemfile::private_key(&mut key_reader)?
            .context("Failed to parse generated private key")?;

        Ok((certs, private_key))
    }
}