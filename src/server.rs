use std::sync::Arc;

use winfsp::host::{FileSystemHost, VolumeParams};
use winfsp::winfsp_init_or_die;

use crate::{error::Result, store::Store, vfs::TagVfs};

pub fn run(store: Arc<Store>, mount: &str) -> Result<()> {
    let _fsp = winfsp_init_or_die();

    let mut params = VolumeParams::new();
    params
        .filesystem_name("brevity")
        .sector_size(512)
        .sectors_per_allocation_unit(8)
        .max_component_length(255)
        .case_preserved_names(true)
        .unicode_on_disk(true)
        .read_only_volume(true)
        .file_info_timeout(1000);

    let vfs = TagVfs::new(store);
    let mut host = FileSystemHost::new(params, vfs)?;
    host.mount(mount)?;
    host.start()?;

    println!("brevity mounted at {mount}");
    println!("Press Enter to unmount and exit.");
    let mut buf = String::new();
    std::io::stdin().read_line(&mut buf)?;

    host.stop();
    host.unmount();
    Ok(())
}
