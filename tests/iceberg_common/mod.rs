//! Shared Iceberg REST catalog fixture for the Iceberg plugin e2e tests.
//!
//! Runs `apache/iceberg-rest-fixture` with a `file://` warehouse. The host
//! directory is bind-mounted at the same path inside the container, so the
//! catalog (metadata files) and the plugin and test code on the host (data
//! files, manifests) see identical paths. The container runs as the host user
//! so both sides can write into each other's directories. A generated
//! `/etc/passwd` names that uid: Hadoop's login fails with "invalid null
//! input: name" for a uid the image does not know (CI runners use 1001; the
//! image only has 1000).
#![allow(dead_code)] // each test binary uses its own subset

use std::collections::HashMap;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::PathBuf;

use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder};
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use std::sync::Arc;
use streamling_e2e::{TestContext, TestContextOptions, init_tracing};
use testcontainers::core::{ContainerPort, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

const IMAGE: &str = "apache/iceberg-rest-fixture";
const TAG: &str = "1.10.1";
const PORT: u16 = 8181;

pub struct IcebergFixture {
    pub ctx: TestContext,
    pub catalog: RestCatalog,
    pub catalog_uri: String,
    /// Namespace unique to this test.
    pub namespace: NamespaceIdent,
    pub warehouse_dir: PathBuf,
    _container: ContainerAsync<GenericImage>,
}

impl IcebergFixture {
    pub async fn setup(options: TestContextOptions) -> IcebergFixture {
        init_tracing();
        let ctx = TestContext::with_options(options)
            .await
            .expect("failed to create test context");

        let warehouse_dir = std::env::temp_dir().join(format!("iceberg-e2e-{}", ctx.test_id));
        std::fs::create_dir_all(&warehouse_dir).expect("create warehouse dir");
        std::fs::set_permissions(&warehouse_dir, std::fs::Permissions::from_mode(0o777))
            .expect("chmod warehouse dir");
        let meta = std::fs::metadata(&warehouse_dir).expect("stat warehouse dir");
        let dir = warehouse_dir.to_str().expect("utf-8 temp dir").to_string();
        let passwd = warehouse_dir.with_extension("passwd");
        std::fs::write(
            &passwd,
            format!(
                "root:x:0:0::/root:/bin/sh\nstreamling:x:{}:{}::/tmp:/bin/sh\n",
                meta.uid(),
                meta.gid()
            ),
        )
        .expect("write passwd");

        let container = GenericImage::new(IMAGE, TAG)
            .with_exposed_port(ContainerPort::Tcp(PORT))
            .with_wait_for(WaitFor::message_on_either_std("Started Server@"))
            .with_env_var("CATALOG_WAREHOUSE", format!("file://{dir}"))
            .with_mount(Mount::bind_mount(dir.clone(), dir.clone()))
            .with_mount(Mount::bind_mount(
                passwd.to_str().expect("utf-8 temp dir"),
                "/etc/passwd",
            ))
            .with_user(format!("{}:{}", meta.uid(), meta.gid()))
            .start()
            .await
            .expect("failed to start the Iceberg REST fixture");
        let port = container
            .get_host_port_ipv4(PORT)
            .await
            .expect("REST fixture port");
        let catalog_uri = format!("http://127.0.0.1:{port}");

        let catalog = RestCatalogBuilder::default()
            .with_storage_factory(Arc::new(OpenDalResolvingStorageFactory::new()))
            .load(
                "test",
                HashMap::from([("uri".to_string(), catalog_uri.clone())]),
            )
            .await
            .expect("connect to the REST fixture");
        let namespace = NamespaceIdent::new(format!("ns_{}", &ctx.test_id[..8]));
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("create namespace");

        IcebergFixture {
            ctx,
            catalog,
            catalog_uri,
            namespace,
            warehouse_dir,
            _container: container,
        }
    }

    pub fn table_ident(&self, name: &str) -> TableIdent {
        TableIdent::new(self.namespace.clone(), name.to_string())
    }

    /// `namespace.table`, the form the plugins' `table` option takes.
    pub fn table_option(&self, name: &str) -> String {
        format!("{}.{name}", self.namespace.as_ref().join("."))
    }

    /// Reads the current snapshot of a table.
    pub async fn read_table(&self, name: &str) -> Vec<RecordBatch> {
        let table = self
            .catalog
            .load_table(&self.table_ident(name))
            .await
            .expect("load table");
        table
            .scan()
            .select_all()
            .build()
            .expect("build scan")
            .to_arrow()
            .await
            .expect("scan to arrow")
            .try_collect()
            .await
            .expect("read table")
    }
}

impl Drop for IcebergFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.warehouse_dir);
        let _ = std::fs::remove_file(self.warehouse_dir.with_extension("passwd"));
    }
}
