use crate::error::AppError;
use crate::tmp_index::{TmpWords, STOP_WORDS};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::str::from_utf8;
use std::time::{Duration, Instant};
use wildmatch::WildMatch;

///
pub struct Words {
    path: PathBuf,
    words: BTreeMap<String, Word>,
    files: Vec<String>,
    age: Instant,
    auto_save: Duration,
}

pub struct Word {
    count: u32,
    file_idx: BTreeSet<u32>,
}

impl Debug for Words {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for (txt, word) in self.words.iter() {
            write!(f, "{}|{}|", txt, word.count)?;
            // for file_idx in word.file_idx.iter() {
            //     write!(f, "{}/", file_idx)?;
            // }
            writeln!(f)?;
        }
        Ok(())
    }
}

impl Word {
    pub fn add_file_idx(&mut self, f_idx: u32) {
        self.file_idx.insert(f_idx);
    }
}

impl Words {
    pub fn new(path: &Path) -> Result<Self, AppError> {
        Ok(Words {
            path: path.into(),
            words: Default::default(),
            files: Default::default(),
            age: Instant::now(),
            auto_save: Duration::from_secs(60),
        })
    }

    pub fn write(&self) -> Result<(), AppError> {
        let tmp = self.path.parent().expect("path").join(".tmp_stored");
        if tmp.exists() {
            return Ok(());
        }

        self.write_to(&tmp)?;

        fs::rename(tmp, &self.path)?;
        Ok(())
    }

    fn write_to(&self, path: &Path) -> Result<(), AppError> {
        let mut f = BufWriter::new(File::create(path)?);

        f.write_all(&(self.files.len() as u32).to_ne_bytes())?;
        for file in self.files.iter() {
            f.write_all(file.as_bytes())?;
            f.write_all(b"\n")?;
        }

        f.write_all(&(self.words.len() as u32).to_ne_bytes())?;
        for (txt, word) in self.words.iter() {
            f.write_all(txt.as_bytes())?;
            f.write_all(b"\n")?;
            f.write_all(&word.count.to_ne_bytes())?;

            f.write_all(&(word.file_idx.len() as u32).to_ne_bytes())?;
            for u in &word.file_idx {
                f.write_all(&u.to_ne_bytes())?;
            }
            f.write_all(b"\n")?;
        }

        Ok(())
    }

    pub fn read(path: &Path) -> Result<Words, AppError> {
        let mut words = Words::new(path)?;

        let mut f = BufReader::new(File::open(path)?);
        let mut buf = Vec::new();
        let mut u = [0u8; 4];
        let mut b = [0u8; 1];

        f.read_exact(&mut u)?;
        let n = u32::from_ne_bytes(u) as usize;
        for _ in 0..n {
            buf.clear();

            f.read_until(b'\n', &mut buf)?;
            buf.pop();
            let file = from_utf8(&buf)?.to_string();
            words.files.push(file);
        }

        f.read_exact(&mut u)?;
        let n = u32::from_ne_bytes(u) as usize;
        for _ in 0..n {
            buf.clear();

            f.read_until(b'\n', &mut buf)?;
            buf.pop();
            let word = from_utf8(&buf)?.to_string();

            f.read_exact(&mut u)?;
            let count = u32::from_ne_bytes(u);

            let mut file_idx = BTreeSet::new();
            f.read_exact(&mut u)?;
            let n = u32::from_ne_bytes(u);
            for _ in 0..n {
                f.read_exact(&mut u)?;
                let idx = u32::from_ne_bytes(u);
                file_idx.insert(idx);
            }
            f.read_exact(&mut b)?;

            words.words.insert(word, Word { count, file_idx });
        }

        Ok(words)
    }

    pub fn add_file(&mut self, file: String) -> u32 {
        match self.files.iter().position(|v| &file == v) {
            None => {
                let idx = self.files.len();
                self.files.push(file);
                idx as u32
            }
            Some(idx) => idx as u32,
        }
    }

    pub fn remove_file(&mut self, file: String) {
        if let Ok(idx) = self.files.binary_search(&file) {
            self.files.remove(idx);

            for word in self.words.values_mut() {
                word.file_idx.remove(&(idx as u32));
            }

            self.words.retain(|_txt, word| !word.file_idx.is_empty());
        }
    }

    pub fn add_word<S: AsRef<str> + Into<String>>(&mut self, word: S, file_idx: u32) {
        if let Ok(_) = STOP_WORDS.binary_search_by(|probe| (*probe).cmp(word.as_ref())) {
            return;
        }

        self.words
            .entry(word.into())
            .and_modify(|v| {
                v.count += 1;
                v.add_file_idx(file_idx)
            })
            .or_insert_with(|| Word {
                count: 1,
                file_idx: {
                    let mut v = BTreeSet::new();
                    v.insert(file_idx);
                    v
                },
            });
    }

    pub fn append(&mut self, other: TmpWords) {
        let f_idx = self.add_file(other.file);

        for a_txt in other.words.into_iter() {
            self.words
                .entry(a_txt)
                .and_modify(|v| {
                    v.count += 1;
                    v.add_file_idx(f_idx);
                })
                .or_insert_with(|| {
                    let mut w = Word {
                        count: 1,
                        file_idx: Default::default(),
                    };
                    w.add_file_idx(f_idx);
                    w
                });
        }
    }

    pub fn have_file(&self, txt: &String) -> bool {
        self.files.contains(txt)
    }

    pub fn find_file(&self, txt: &str) -> BTreeSet<&String> {
        let find = WildMatch::new(txt);
        self.files
            .iter()
            .filter(|v| find.matches(v.as_str()))
            .collect()
    }

    pub fn find(&self, txt: &[&str]) -> Result<BTreeSet<String>, AppError> {
        let mut collect_idx = BTreeSet::new();

        let mut first = true;
        for t in txt {
            let match_find = WildMatch::new(t);

            let mut f_idx = BTreeSet::new();
            for (_, word) in self.words.iter().filter(|(txt, _)| match_find.matches(txt)) {
                for ff in word.file_idx.iter() {
                    if first {
                        f_idx.insert(*ff);
                    } else {
                        if collect_idx.contains(ff) {
                            f_idx.insert(*ff);
                        }
                    }
                }
            }

            first = false;
            collect_idx = f_idx;
        }

        Ok(collect_idx
            .iter()
            .map(|v| self.files.get(*v as usize).expect("file"))
            .cloned()
            .collect())
    }

    pub fn should_auto_save(&mut self) -> bool {
        if self.age.elapsed() > self.auto_save {
            self.age = Instant::now();
            true
        } else {
            false
        }
    }

    pub fn set_auto_save_interval(&mut self, auto_save: Duration) {
        self.auto_save = auto_save;
    }
}
