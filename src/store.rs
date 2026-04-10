use std::{
    collections::HashSet,
    fs,
    io::{self, ErrorKind},
    path::{Path, PathBuf},
    time::SystemTime,
};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use sha2::{Digest, Sha256};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    pub id: Uuid,
    pub name: String,
    pub object: String,
    pub created_at: u64,
    pub tags: Vec<String>,
}

pub struct Store {
    root: PathBuf,
}

impl Store {
    pub fn open(root: impl Into<PathBuf>) -> io::Result<Self> {
        let root = root.into();
        fs::create_dir_all(root.join("blobs"))?;
        fs::create_dir_all(root.join("registry"))?;
        fs::create_dir_all(root.join("tags"))?;
        Ok(Self { root })
    }

    fn object_path(&self, hash: &str) -> PathBuf {
        self.root.join("blobs").join(&hash[..2]).join(&hash[2..])
    }

    fn entry_path(&self, id: Uuid) -> PathBuf {
        self.root.join("registry").join(format!("{}.toml", id))
    }

    fn tag_dir(&self, tag: &str) -> PathBuf {
        self.root.join("tags").join(tag)
    }

    pub fn import(&self, src: &Path, tags: &[String]) -> Result<FileEntry> {
        let content = fs::read(src)?;
        let object = self.store_object(&content)?;
        let name = src
            .file_name()
            .ok_or(Error::PathHasNoFileName)?
            .to_string_lossy()
            .into_owned();
        let id = Uuid::new_v4();
        let created_at = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let entry = FileEntry { id, name, object, created_at, tags: tags.to_vec() };
        self.write_entry(&entry)?;
        for tag in tags {
            self.link_tag(id, tag)?;
        }
        Ok(entry)
    }

    fn store_object(&self, content: &[u8]) -> io::Result<String> {
        let hash = hex::encode(Sha256::digest(content));
        let path = self.object_path(&hash);
        fs::create_dir_all(path.parent().unwrap())?;
        if !path.exists() {
            fs::write(&path, content)?;
        }
        Ok(hash)
    }

    pub fn read_object(&self, hash: &str) -> io::Result<Vec<u8>> {
        fs::read(self.object_path(hash))
    }

    pub fn object_size(&self, hash: &str) -> io::Result<u64> {
        Ok(fs::metadata(self.object_path(hash))?.len())
    }

    fn write_entry(&self, entry: &FileEntry) -> Result<()> {
        let path = self.entry_path(entry.id);
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, toml::to_string(entry)?)?;
        fs::rename(tmp, path)?;
        Ok(())
    }

    pub fn load_entry(&self, id: Uuid) -> Result<FileEntry> {
        Ok(toml::from_str(&fs::read_to_string(self.entry_path(id))?)?)
    }

    fn link_tag(&self, file_id: Uuid, tag: &str) -> io::Result<()> {
        let dir = self.tag_dir(tag);
        fs::create_dir_all(&dir)?;
        fs::write(dir.join(file_id.to_string()), [])?;
        Ok(())
    }

    fn unlink_tag(&self, file_id: Uuid, tag: &str) -> io::Result<()> {
        let path = self.tag_dir(tag).join(file_id.to_string());
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    pub fn tag_members(&self, tag: &str) -> io::Result<HashSet<Uuid>> {
        let dir = self.tag_dir(tag);
        let iter = match fs::read_dir(dir) {
            Ok(it) => it,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(HashSet::new()),
            Err(e) => return Err(e),
        };
        let mut ids = HashSet::new();
        for entry in iter {
            let entry = entry?;
            if let Ok(id) = entry.file_name().to_string_lossy().parse::<Uuid>() {
                ids.insert(id);
            }
        }
        Ok(ids)
    }

    pub fn all_tags(&self) -> io::Result<Vec<String>> {
        let mut tags = Vec::new();
        for entry in fs::read_dir(self.root.join("tags"))? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                tags.push(entry.file_name().to_string_lossy().into_owned());
            }
        }
        Ok(tags)
    }

    pub fn query(&self, tags: &[String]) -> Result<Vec<FileEntry>> {
        let ids = self.files_matching_tags(tags)?;
        let mut entries = Vec::new();
        for id in ids {
            // Skip entries whose metadata can't be read
            if let Ok(e) = self.load_entry(id) {
                entries.push(e);
            }
        }
        Ok(entries)
    }

    fn all_file_ids(&self) -> io::Result<HashSet<Uuid>> {
        fs::read_dir(self.root.join("registry"))?.try_fold(HashSet::new(), |mut ids, entry| {
            let entry = entry?;
            let name = entry.file_name();
            let s = name.to_string_lossy();
            if let Some(stem) = s.strip_suffix(".toml")
                && let Ok(id) = stem.parse::<Uuid>()
            {
                ids.insert(id);
            }
            Ok(ids)
        })
    }

    fn files_matching_tags(&self, tags: &[String]) -> Result<HashSet<Uuid>> {
        if tags.is_empty() {
            return Ok(self.all_file_ids()?);
        }

        let (first, rest) = tags.split_first().expect("tags is not empty");

        rest.iter().try_fold(self.tag_members(first)?, |mut cum, tag| {
            let next = self.tag_members(tag)?;
            cum.retain(|id| next.contains(id));
            Ok(cum)
        })
    }

    pub fn add_tags(&self, id: Uuid, new_tags: &[String]) -> Result<()> {
        let mut entry = self.load_entry(id)?;
        for tag in new_tags {
            if !entry.tags.contains(tag) {
                entry.tags.push(tag.clone());
                self.link_tag(id, tag)?;
            }
        }
        self.write_entry(&entry)
    }

    pub fn remove_tags(&self, id: Uuid, rm_tags: &[String]) -> Result<()> {
        let mut entry = self.load_entry(id)?;
        for tag in rm_tags {
            entry.tags.retain(|t| t != tag);
            self.unlink_tag(id, tag)?;
        }
        self.write_entry(&entry)
    }
}
