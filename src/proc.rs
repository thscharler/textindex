use crate::index::index_txt;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use walkdir::WalkDir;

pub fn update_index(p: &Path) -> Result<(), anyhow::Error> {
    let mut buf = Vec::new();

    if p.exists() && p.is_dir() {
        for entry in WalkDir::new(p).into_iter().flatten() {
            if entry.metadata()?.is_dir() {
                // let absolute = entry.path();
                // let relative = entry.path().strip_prefix(p)?;
            } else {
                let absolute = entry.path();
                let relative = entry.path().strip_prefix(p)?;

                println!("Index {:?}", relative);
                buf.clear();
                File::open(absolute)?.read_to_end(&mut buf)?;

                let w = index_txt(absolute);
            }
        }
    }

    Ok(())
}
