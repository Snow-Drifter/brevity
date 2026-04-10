use std::{
    collections::{HashMap, HashSet},
    ffi::c_void,
    io::ErrorKind,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::SystemTime,
};

use winfsp::{
    filesystem::{
        DirBuffer, DirInfo, DirMarker, FileSecurity, FileInfo, FileSystemContext, OpenFileInfo,
        VolumeInfo, WideNameInfo,
    },
    FspError, U16CStr,
};

use crate::store::{FileEntry, Store};

const FILE_ATTRIBUTE_READONLY: u32 = 0x0000_0001;
const FILE_ATTRIBUTE_DIRECTORY: u32 = 0x0000_0010;

fn unix_to_filetime(secs: u64) -> u64 {
    const EPOCH_DIFF_100NS: u64 = 116_444_736_000_000_000;
    secs.saturating_mul(10_000_000).saturating_add(EPOCH_DIFF_100NS)
}

fn now_filetime() -> u64 {
    let secs = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    unix_to_filetime(secs)
}

fn display_name(entry: &FileEntry, has_dup: bool) -> String {
    if !has_dup {
        return entry.name.clone();
    }
    let short = &entry.id.to_string()[..8];
    match entry.name.rfind('.') {
        Some(dot) => format!("{}_{}{}", &entry.name[..dot], short, &entry.name[dot..]),
        None => format!("{}_{}", entry.name, short),
    }
}

fn name_count_map(files: &[FileEntry]) -> HashMap<String, usize> {
    let mut map = HashMap::new();
    for f in files {
        *map.entry(f.name.clone()).or_insert(0usize) += 1;
    }
    map
}

fn parse_vfs_path<'a>(
    path: &'a str,
    known_tags: &[String],
) -> (Vec<String>, Option<&'a str>) {
    let parts: Vec<&str> = path.split('\\').filter(|s| !s.is_empty()).collect();

    if parts.is_empty() {
        return (vec![], None);
    }

    let last = *parts.last().unwrap();
    let prefix: Vec<String> = parts[..parts.len() - 1]
        .iter()
        .map(|s| s.to_string())
        .collect();

    if known_tags.contains(&last.to_string()) {
        let mut tags = prefix;
        tags.push(last.to_string());
        (tags, None)
    } else {
        (prefix, Some(last))
    }
}

pub struct TagDirContext {
    tags: Vec<String>,
    dir_buffer: DirBuffer,
    info: FileInfo,
    filled: AtomicBool,
}

pub struct TagFileContext {
    content: Vec<u8>,
    info: FileInfo,
}

pub enum TagContext {
    Dir(TagDirContext),
    File(TagFileContext),
}

pub struct TagVfs {
    store: Arc<Store>,
}

impl TagVfs {
    pub fn new(store: Arc<Store>) -> Self {
        Self { store }
    }

    fn make_dir_info(&self) -> FileInfo {
        let now = now_filetime();
        FileInfo {
            file_attributes: FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_READONLY,
            reparse_tag: 0,
            allocation_size: 0,
            file_size: 0,
            creation_time: now,
            last_access_time: now,
            last_write_time: now,
            change_time: now,
            index_number: 0,
            hard_links: 0,
            ea_size: 0,
        }
    }

    fn make_file_info(size: u64, ts: u64) -> FileInfo {
        FileInfo {
            file_attributes: FILE_ATTRIBUTE_READONLY,
            reparse_tag: 0,
            allocation_size: (size + 4095) & !4095,
            file_size: size,
            creation_time: ts,
            last_access_time: ts,
            last_write_time: ts,
            change_time: ts,
            index_number: 0,
            hard_links: 0,
            ea_size: 0,
        }
    }
}

impl FileSystemContext for TagVfs {
    type FileContext = TagContext;

    fn get_security_by_name(
        &self,
        file_name: &U16CStr,
        _security_descriptor: Option<&mut [c_void]>,
        _reparse_point_resolver: impl FnOnce(&U16CStr) -> Option<FileSecurity>,
    ) -> winfsp::Result<FileSecurity> {
        let path = file_name.to_string_lossy();
        let known_tags = self
            .store
            .all_tags()
            .map_err(|e| FspError::IO(e.kind()))?;
        let (tags, filename) = parse_vfs_path(&path, &known_tags);

        let attributes = match filename {
            None => FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_READONLY,
            Some(name) => {
                let files = self
                    .store
                    .query(&tags)
                    .map_err(|_| FspError::IO(ErrorKind::Other))?;
                let counts = name_count_map(&files);
                let found = files
                    .iter()
                    .any(|f| display_name(f, counts[&f.name] > 1) == name);
                if !found {
                    return Err(FspError::IO(ErrorKind::NotFound));
                }
                FILE_ATTRIBUTE_READONLY
            }
        };

        Ok(FileSecurity {
            reparse: false,
            sz_security_descriptor: 0,
            attributes,
        })
    }

    fn open(
        &self,
        file_name: &U16CStr,
        _create_options: u32,
        _granted_access: u32,
        file_info: &mut OpenFileInfo,
    ) -> winfsp::Result<Self::FileContext> {
        let path = file_name.to_string_lossy();
        let known_tags = self
            .store
            .all_tags()
            .map_err(|e| FspError::IO(e.kind()))?;
        let (tags, filename) = parse_vfs_path(&path, &known_tags);

        match filename {
            None => {
                let info = self.make_dir_info();
                *file_info.as_mut() = info.clone();
                Ok(TagContext::Dir(TagDirContext {
                    tags,
                    dir_buffer: DirBuffer::new(),
                    info,
                    filled: AtomicBool::new(false),
                }))
            }
            Some(name) => {
                let files = self
                    .store
                    .query(&tags)
                    .map_err(|_| FspError::IO(ErrorKind::Other))?;
                let counts = name_count_map(&files);
                let entry = files
                    .into_iter()
                    .find(|f| display_name(f, counts[&f.name] > 1) == name)
                    .ok_or(FspError::IO(ErrorKind::NotFound))?;

                let content = self
                    .store
                    .read_object(&entry.object)
                    .map_err(|e| FspError::IO(e.kind()))?;
                let size = content.len() as u64;
                let ts = unix_to_filetime(entry.created_at);
                let info = Self::make_file_info(size, ts);
                *file_info.as_mut() = info.clone();
                Ok(TagContext::File(TagFileContext { content, info }))
            }
        }
    }

    fn close(&self, _context: Self::FileContext) {}

    fn get_file_info(
        &self,
        context: &Self::FileContext,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<()> {
        *file_info = match context {
            TagContext::Dir(d) => d.info.clone(),
            TagContext::File(f) => f.info.clone(),
        };
        Ok(())
    }

    fn read(
        &self,
        context: &Self::FileContext,
        buffer: &mut [u8],
        offset: u64,
    ) -> winfsp::Result<u32> {
        let file = match context {
            TagContext::File(f) => f,
            TagContext::Dir(_) => return Err(FspError::IO(ErrorKind::Other)),
        };

        let start = offset as usize;
        if start >= file.content.len() {
            return Ok(0);
        }
        let src = &file.content[start..];
        let n = src.len().min(buffer.len());
        buffer[..n].copy_from_slice(&src[..n]);
        Ok(n as u32)
    }

    fn read_directory(
        &self,
        context: &Self::FileContext,
        _pattern: Option<&U16CStr>,
        marker: DirMarker<'_>,
        buffer: &mut [u8],
    ) -> winfsp::Result<u32> {
        let dir = match context {
            TagContext::Dir(d) => d,
            TagContext::File(_) => return Err(FspError::IO(ErrorKind::Other)),
        };

        if !dir.filled.load(Ordering::Relaxed) {
            if let Ok(lock) = dir.dir_buffer.acquire(true, None) {
                let files = self
                    .store
                    .query(&dir.tags)
                    .map_err(|_| FspError::IO(ErrorKind::Other))?;

                let now = now_filetime();

                let mut sub_tags: HashSet<String> = HashSet::new();
                for file in &files {
                    for tag in &file.tags {
                        if !dir.tags.contains(tag) {
                            sub_tags.insert(tag.clone());
                        }
                    }
                }

                let mut entries: Vec<(String, FileInfo)> = Vec::new();

                entries.push((".".to_string(), dir.info.clone()));

                let placeholder_dir = FileInfo {
                    file_attributes: FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_READONLY,
                    file_size: 0,
                    allocation_size: 0,
                    creation_time: now,
                    last_access_time: now,
                    last_write_time: now,
                    change_time: now,
                    ..FileInfo::default()
                };
                entries.push(("..".to_string(), placeholder_dir.clone()));

                for tag in sub_tags {
                    entries.push((tag, placeholder_dir.clone()));
                }

                let counts = name_count_map(&files);
                for file in &files {
                    let has_dup = counts[&file.name] > 1;
                    let display = display_name(file, has_dup);
                    let size = self.store.object_size(&file.object).unwrap_or(0);
                    let ts = unix_to_filetime(file.created_at);
                    entries.push((display, Self::make_file_info(size, ts)));
                }

                entries.sort_by(|a, b| {
                    a.0.to_lowercase().cmp(&b.0.to_lowercase())
                });

                for (name, info) in entries {
                    let mut di: DirInfo = DirInfo::new();
                    let wide: Vec<u16> = name.encode_utf16().collect();
                    di.set_name_raw(wide.as_slice())?;
                    *di.file_info_mut() = info;
                    lock.write(&mut di)?;
                }
            }
            dir.filled.store(true, Ordering::Relaxed);
        }

        Ok(dir.dir_buffer.read(marker, buffer))
    }

    fn get_volume_info(&self, out: &mut VolumeInfo) -> winfsp::Result<()> {
        out.total_size = 1024 * 1024 * 1024;
        out.free_size = 0;
        out.set_volume_label("brevity");
        Ok(())
    }
}
