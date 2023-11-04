use crate::error::AppError;
use crate::index2::tmp_index::{index_html, index_txt, TmpWords};
use crate::index2::Words;
use crossbeam::channel::{bounded, Receiver, Sender, TryRecvError};
use rustyline::ExternalPrinter;
use std::borrow::Cow;
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::io::Write;
use std::iter::Flatten;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::thread::{sleep, JoinHandle};
use std::time::{Duration, Instant};
use walkdir::WalkDir;
use wildmatch::WildMatch;

#[derive(Debug)]
pub enum Msg {
    Quit,
    WalkTree(PathBuf),
    WalkFinished(PathBuf),
    Load(u32, FileFilter, PathBuf, String),
    Index(u32, FileFilter, PathBuf, String, String),
    MergeWords(u32, TmpWords),
    DeleteFile(String),
    Debug,
    AutoSave,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum FileFilter {
    Ignore,
    Inspect,
    Text,
    Html,
}

#[derive(Default)]
pub struct Found {
    pub terms: Vec<String>,

    pub files: Vec<String>,

    pub lines_idx: usize,
    pub lines: Vec<(String, Vec<String>)>,
}

pub struct Data {
    pub words: RwLock<Words>,
    pub found: Mutex<Found>,
    pub log: File,
}

impl Data {
    pub fn write(&'static self) -> Result<(), AppError> {
        if let Ok(mut wrl) = self.words.write() {
            wrl.write()?;
            Ok(())
        } else {
            Ok(())
        }
    }

    pub fn read(path: &Path) -> Result<&'static Data, AppError> {
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open("log.txt")?;

        let words = Words::read(path)?;

        let data: &'static Data = Box::leak(Box::new(Data {
            words: RwLock::new(words),
            found: Default::default(),
            log,
        }));

        Ok(data)
    }
}

#[derive(Default)]
pub struct WorkerState {
    pub state: u64,
    pub msg: String,
}

pub struct Worker {
    pub name: &'static str,
    pub handle: JoinHandle<()>,
    pub state: Arc<Mutex<WorkerState>>,
}

impl Worker {
    pub fn new(name: &'static str, handle: JoinHandle<()>, state: Arc<Mutex<WorkerState>>) -> Self {
        Self {
            name,
            handle,
            state,
        }
    }
}

pub struct Work {
    pub send: Sender<Msg>,
    pub recv_send: [(Receiver<Msg>, Sender<Msg>); 4],
    pub recv: Receiver<Msg>,
    pub workers: [Worker; 8],

    pub printer: Arc<Mutex<dyn ExternalPrinter + Send>>,
}

pub fn init_work<P: ExternalPrinter + Send + Sync + 'static>(
    printer: P,
    data: &'static Data,
) -> Work {
    let printer = Arc::new(Mutex::new(printer));

    let (s0, r1) = bounded::<Msg>(10);
    let (s1, r2) = bounded::<Msg>(10);
    let (s2, r3) = bounded::<Msg>(10);
    let (s3, r4) = bounded::<Msg>(10);
    let (s4, r5) = bounded::<Msg>(10);

    let n1 = "walking";
    let st1 = Arc::new(Mutex::new(WorkerState::default()));
    let h1 = spawn_walking(
        r1.clone(),
        s1.clone(),
        Arc::clone(&st1),
        data,
        printer.clone(),
    );
    let n2 = "loading";
    let st2 = Arc::new(Mutex::new(WorkerState::default()));
    let h2 = spawn_loading(
        r2.clone(),
        s2.clone(),
        Arc::clone(&st2),
        data,
        printer.clone(),
    );
    let n3_1 = "index 1";
    let st3_1 = Arc::new(Mutex::new(WorkerState::default()));
    let h3_1 = spawn_indexing(
        r3.clone(),
        s3.clone(),
        Arc::clone(&st3_1),
        data,
        printer.clone(),
    );
    let n3_2 = "index 2";
    let st3_2 = Arc::new(Mutex::new(WorkerState::default()));
    let h3_2 = spawn_indexing(
        r3.clone(),
        s3.clone(),
        Arc::clone(&st3_2),
        data,
        printer.clone(),
    );
    let n3_3 = "index 3";
    let st3_3 = Arc::new(Mutex::new(WorkerState::default()));
    let h3_3 = spawn_indexing(
        r3.clone(),
        s3.clone(),
        Arc::clone(&st3_3),
        data,
        printer.clone(),
    );
    let n3_4 = "index 4";
    let st3_4 = Arc::new(Mutex::new(WorkerState::default()));
    let h3_4 = spawn_indexing(
        r3.clone(),
        s3.clone(),
        Arc::clone(&st3_4),
        data,
        printer.clone(),
    );
    let n4 = "merge";
    let st4 = Arc::new(Mutex::new(WorkerState::default()));
    let h4 = spawn_merge_words(
        r4.clone(),
        s4.clone(),
        Arc::clone(&st4),
        data,
        printer.clone(),
    );
    let n5 = "terminal";
    let st5 = Arc::new(Mutex::new(WorkerState::default()));
    let h5 = spawn_terminal(r5.clone(), Arc::clone(&st5), data, printer.clone());

    Work {
        send: s0,
        recv_send: [(r1, s1), (r2, s2), (r3, s3), (r4, s4)],
        recv: r5,
        workers: [
            Worker::new(n1, h1, st1),
            Worker::new(n2, h2, st2),
            Worker::new(n3_1, h3_1, st3_1),
            Worker::new(n3_2, h3_2, st3_2),
            Worker::new(n3_3, h3_3, st3_3),
            Worker::new(n3_4, h3_4, st3_4),
            Worker::new(n4, h4, st4),
            Worker::new(n5, h5, st5),
        ],
        printer,
    }
}

pub fn shut_down(work: &Work) {
    println!("sending shutdown!");
    if let Err(e) = work.send.send(Msg::Quit) {
        if let Ok(mut print) = work.printer.lock() {
            let _ = print.print(format!("shutdown {:?}", e));
        }
    }

    if let Ok(mut print) = work.printer.lock() {
        let _ = print.print("wait on shutdown".into());
    }

    sleep(Duration::from_millis(100));

    for w in work.workers.iter() {
        if !w.handle.is_finished() {
            continue;
        }
    }
}

fn spawn_walking(
    recv: Receiver<Msg>,
    send: Sender<Msg>,
    state: Arc<Mutex<WorkerState>>,
    data: &'static Data,
    printer: Arc<Mutex<dyn ExternalPrinter + Send>>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        print_err_(
            &printer,
            data.log.try_clone().unwrap(),
            "walker",
            walk_proc(recv, send, state, data, &printer),
        );
    })
}

struct WalkingProc {
    path: PathBuf,
    tree_iter: Flatten<walkdir::IntoIter>,
    count: u32,
}

fn walk_proc(
    recv: Receiver<Msg>,
    send: Sender<Msg>,
    state: Arc<Mutex<WorkerState>>,
    data: &'static Data,
    printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
) -> Result<(), AppError> {
    // This is a bit more complicated, as we need to keep up the message flow
    // while traversing the directory tree. We interweave each step of the tree iteration
    // and message processing.

    let mut proc = None;

    loop {
        match &mut proc {
            None => match recv.recv()? {
                Msg::Quit => {
                    state.lock().unwrap().state = 1;
                    send.send(Msg::Quit)?;
                    break;
                }
                Msg::Debug => {
                    state.lock().unwrap().state = 2;
                    print_(printer, "walk_tree empty");
                    send.send(Msg::Debug)?;
                }
                Msg::WalkTree(path) => {
                    state.lock().unwrap().state = 3;
                    proc = Some(WalkingProc {
                        path: path.clone(),
                        tree_iter: WalkDir::new(path).into_iter().flatten(),
                        count: 0,
                    });
                }
                msg => {
                    state.lock().unwrap().state = 4;
                    send.send(msg)?;
                }
            },
            Some(rproc) => {
                match recv.try_recv() {
                    Ok(Msg::Quit) => {
                        state.lock().unwrap().state = 5;
                        send.send(Msg::Quit)?;
                        break;
                    }
                    Ok(Msg::Debug) => {
                        state.lock().unwrap().state = 6;
                        print_(printer, format!("walk_tree {}", rproc.count));
                        send.send(Msg::Debug)?;
                    }
                    Ok(Msg::WalkTree(_)) => {
                        state.lock().unwrap().state = 7;
                        if let Ok(mut print) = printer.lock() {
                            let _ = print.print(
                                "new tree walk ignored, still working on the last one.".to_string(),
                            );
                        }
                    }
                    Ok(msg) => {
                        state.lock().unwrap().state = 8;
                        send.send(msg)?;
                    }
                    Err(TryRecvError::Empty) => {
                        state.lock().unwrap().state = 9;
                    }
                    Err(TryRecvError::Disconnected) => {
                        state.lock().unwrap().state = 10;
                        break;
                    }
                }

                if let Some(entry) = rproc.tree_iter.next() {
                    state.lock().unwrap().state = 101;
                    let meta = entry.metadata()?;
                    if meta.is_file() {
                        let absolute = entry.path();
                        let relative = entry
                            .path()
                            .strip_prefix(&rproc.path)
                            .unwrap_or(absolute)
                            .to_string_lossy()
                            .to_string();

                        let filter = name_filter(absolute);

                        if filter == FileFilter::Ignore {
                            // print_(&printer, format!("ignore {:?}", relative));
                            continue;
                        } else {
                            // print_(&printer, format!("process {:?}", relative));
                        }

                        let do_send = {
                            state.lock().unwrap().state = 102;
                            let words = data.words.read()?;
                            !words.have_file(&relative)
                        };
                        if do_send {
                            state.lock().unwrap().state = 103;
                            rproc.count += 1;
                            send.send(Msg::Load(rproc.count, filter, absolute.into(), relative))?;
                        }
                    }
                } else {
                    state.lock().unwrap().state = 104;
                    send.send(Msg::AutoSave)?;
                    state.lock().unwrap().state = 105;
                    send.send(Msg::WalkFinished(rproc.path.clone()))?;
                    proc = None;
                }
            }
        }
    }

    Ok(())
}

fn spawn_loading(
    recv: Receiver<Msg>,
    send: Sender<Msg>,
    state: Arc<Mutex<WorkerState>>,
    data: &'static Data,
    printer: Arc<Mutex<dyn ExternalPrinter + Send>>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        print_err_(
            &printer,
            data.log.try_clone().unwrap(),
            "loading",
            load_proc(recv, send, state, data, &printer),
        );
    })
}

fn load_proc(
    recv: Receiver<Msg>,
    send: Sender<Msg>,
    state: Arc<Mutex<WorkerState>>,
    _data: &'static Data,
    printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
) -> Result<(), AppError> {
    let mut last_count = 0;

    loop {
        match recv.recv()? {
            Msg::Quit => {
                state.lock().unwrap().state = 1;
                send.send(Msg::Quit)?;
                break;
            }
            Msg::Debug => {
                state.lock().unwrap().state = 2;
                print_(printer, format!("loading {}", last_count));
                send.send(Msg::Debug)?;
            }
            Msg::Load(count, filter, absolute, relative) => {
                state.lock().unwrap().state = 3;
                last_count = count;
                let (filter, txt) = load_file(filter, &absolute)?;
                if filter != FileFilter::Ignore {
                    send.send(Msg::Index(count, filter, absolute, relative, txt))?;
                }
            }
            msg => {
                state.lock().unwrap().state = 4;
                send.send(msg)?;
            }
        }
    }
    Ok(())
}

pub fn load_file(filter: FileFilter, absolute: &Path) -> Result<(FileFilter, String), AppError> {
    let mut buf = Vec::new();
    File::open(&absolute)?.read_to_end(&mut buf)?;
    let str = String::from_utf8_lossy(buf.as_slice());
    let filter = content_filter(filter, str.as_ref());

    Ok((filter, str.into()))
}

fn spawn_indexing(
    recv: Receiver<Msg>,
    send: Sender<Msg>,
    state: Arc<Mutex<WorkerState>>,
    data: &'static Data,
    printer: Arc<Mutex<dyn ExternalPrinter + Send>>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        print_err_(
            &printer,
            data.log.try_clone().unwrap(),
            "indexing",
            index_proc(recv, send, state, data, &printer),
        );
    })
}

fn index_proc(
    recv: Receiver<Msg>,
    send: Sender<Msg>,
    state: Arc<Mutex<WorkerState>>,
    _data: &'static Data,
    printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
) -> Result<(), AppError> {
    let mut last_count = 0;

    loop {
        match recv.recv()? {
            Msg::Quit => {
                state.lock().unwrap().state = 1;
                send.send(Msg::Quit)?;
                break;
            }
            Msg::Debug => {
                state.lock().unwrap().state = 2;
                print_(printer, format!("indexing {}", last_count));
                send.send(Msg::Debug)?;
            }
            Msg::Index(count, filter, _absolute, relative, txt) => {
                state.lock().unwrap().state = 3;
                last_count = count;
                let words = indexing(filter, &relative, &txt);
                send.send(Msg::MergeWords(count, words))?;
            }
            msg => {
                state.lock().unwrap().state = 4;
                send.send(msg)?;
            }
        }
    }
    Ok(())
}

pub fn indexing(filter: FileFilter, relative: &str, txt: &str) -> TmpWords {
    let mut words = TmpWords::new(relative);

    match filter {
        FileFilter::Text => {
            index_txt(&mut words, txt);
        }
        FileFilter::Html => {
            index_html(&mut words, txt);
        }
        FileFilter::Ignore => {}
        FileFilter::Inspect => {}
    }

    words
}

fn spawn_merge_words(
    recv: Receiver<Msg>,
    send: Sender<Msg>,
    state: Arc<Mutex<WorkerState>>,
    data: &'static Data,
    printer: Arc<Mutex<dyn ExternalPrinter + Send>>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        print_err_(
            &printer,
            data.log.try_clone().unwrap(),
            "merge_words",
            merge_words_proc(recv, send, state, data, &printer),
        )
    })
}

fn merge_words_proc(
    recv: Receiver<Msg>,
    send: Sender<Msg>,
    state: Arc<Mutex<WorkerState>>,
    data: &'static Data,
    printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
) -> Result<(), AppError> {
    let mut last_count = 0;

    loop {
        match recv.recv()? {
            Msg::Quit => {
                state.lock().unwrap().state = 1;
                send.send(Msg::Quit)?;
                break;
            }
            Msg::Debug => {
                state.lock().unwrap().state = 2;
                print_(printer, format!("merge words {}", last_count));
                send.send(Msg::Debug)?;
            }
            Msg::MergeWords(count, words) => {
                state.lock().unwrap().state = 3;
                last_count = count;
                print_err_(
                    printer,
                    data.log.try_clone().unwrap(),
                    "merge_words",
                    merge_words(data, &state, words, printer),
                );
            }
            msg => {
                state.lock().unwrap().state = 4;
                send.send(msg)?;
            }
        }
    }
    Ok(())
}

fn merge_words(
    data: &'static Data,
    state: &Arc<Mutex<WorkerState>>,
    words_buffer: TmpWords,
    printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
) -> Result<(), AppError> {
    let do_auto_save = {
        state.lock().unwrap().state = 100;
        let mut write = data.words.write()?;
        state.lock().unwrap().state = 101;
        timing(printer, "merge", 100, || write.append(words_buffer))?;
        state.lock().unwrap().state = 102;
        write.should_auto_save()
    };

    if do_auto_save {
        state.lock().unwrap().state = 200;
        timing(printer, "autosave", 1, || auto_save(printer, data))?;
    }

    Ok(())
}

fn spawn_terminal(
    recv: Receiver<Msg>,
    state: Arc<Mutex<WorkerState>>,
    data: &'static Data,
    printer: Arc<Mutex<dyn ExternalPrinter + Send>>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        print_err_(
            &printer,
            data.log.try_clone().unwrap(),
            "terminal",
            terminal_proc(&recv, state, data, &printer),
        );
    })
}

fn terminal_proc(
    recv: &Receiver<Msg>,
    state: Arc<Mutex<WorkerState>>,
    data: &'static Data,
    printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
) -> Result<(), AppError> {
    loop {
        match recv.recv()? {
            Msg::Quit => {
                state.lock().unwrap().state = 1;
                break;
            }
            Msg::Debug => {
                state.lock().unwrap().state = 2;
                print_(printer, "terminal");
            }
            Msg::AutoSave => {
                state.lock().unwrap().state = 3;
                print_err_(
                    printer,
                    data.log.try_clone().unwrap(),
                    "auto_save",
                    auto_save(printer, data),
                );
            }
            Msg::DeleteFile(file) => {
                state.lock().unwrap().state = 4;
                print_err_(
                    printer,
                    data.log.try_clone().unwrap(),
                    "delete_file",
                    delete_file(printer, data, file),
                );
            }
            Msg::WalkFinished(file) => {
                state.lock().unwrap().state = 5;

                print_(printer, "*** final store ***");

                let mut words = data.words.write()?;
                words.write()?;
                words.compact_blocks();

                print_(printer, format!("*** {:?} finished ***", file));
            }
            msg => {
                state.lock().unwrap().state = 6;
                print_(printer, format!("invalid terminal message {:?}", msg));
            }
        }
    }
    Ok(())
}

pub fn name_filter(path: &Path) -> FileFilter {
    let ext = path
        .extension()
        .map(|v| v.to_string_lossy())
        .unwrap_or(Cow::Borrowed(""))
        .to_lowercase();
    let name = path
        .file_name()
        .map(|v| v.to_string_lossy())
        .unwrap_or(Cow::Borrowed(""))
        .to_lowercase();

    const EXT_IGNORE: &[&str] = &[
        "jpg", "pdf", "gif", "css", "png", "doc", "rtf", "js", "ico", "woff", "zip", "jpeg", "odt",
        "docx", "lit", "xml",
    ];
    const NAME_IGNORE: &[&str] = &[
        ".message.ftp.txt",
        "history.txt",
        ".stored",
        ".tmp_stored",
        "thumbs.db",
        "index.html",
        "jan.html",
        "feb.html",
        "mar.html",
        "apr.html",
        "may.html",
        "jun.html",
        "jul.html",
        "aug.html",
        "sep.html",
        "oct.html",
        "nov.html",
        "dec.html",
        "ctur_seven2^4.html",
        "my_hot_little_sister.html",
    ];
    const PREFIX_IGNORE: &[&str] = &["week"];

    if EXT_IGNORE.contains(&ext.as_str())
        || NAME_IGNORE.contains(&name.as_str())
        || PREFIX_IGNORE.iter().any(|v| name.starts_with(*v))
    {
        FileFilter::Ignore
    } else {
        FileFilter::Inspect
    }
}

pub fn content_filter(filter: FileFilter, txt: &str) -> FileFilter {
    if filter == FileFilter::Ignore {
        return filter;
    }

    const HTML_RECOGNIZE: &[&str] = &[
        "<!--ADULTSONLY",
        "<HTML",
        "<html",
        "<?xml",
        "<!DOCTYPE",
        "<!doctype",
        "_<!DOCTYPE",
    ];

    if HTML_RECOGNIZE.iter().any(|v| txt.starts_with(*v)) {
        FileFilter::Html
    } else {
        FileFilter::Text
    }
}

pub fn auto_save(
    _printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
    data: &'static Data,
) -> Result<(), AppError> {
    data.write()?;
    Ok(())
}

fn delete_file(
    _printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
    data: &'static Data,
    file: String,
) -> Result<(), AppError> {
    let mut write = data.words.write()?;
    write.remove_file(file);

    Ok(())
}

fn print_<S: Into<String>>(printer: &Arc<Mutex<dyn ExternalPrinter + Send>>, msg: S) {
    if let Ok(mut print) = printer.lock() {
        let _ = print.print(msg.into());
    }
}

fn print_err_(
    printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
    mut log: File,
    task: &str,
    res: Result<(), AppError>,
) {
    if let Err(err) = res {
        let _ = writeln!(log, "{} {:#?}", task, err);
        if let Ok(mut print) = printer.lock() {
            let _ = print.print(format!("{} {:?}", task, err));
        }
    }
}

pub fn timing<S: AsRef<str>, R>(
    printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
    name: S,
    threshold: u64,
    fun: impl FnOnce() -> R,
) -> R {
    let now = Instant::now();

    let result = fun();

    let timing = now.elapsed();
    if timing > Duration::from_millis(threshold) {
        print_(printer, format!("{} {:?}", name.as_ref(), now.elapsed()));
    }

    result
}

// Search the result files and return matching text-lines.
pub fn find_matched_lines(
    terms: &[String],
    files: &Vec<String>,
) -> Result<Vec<(String, Vec<String>)>, AppError> {
    let terms: Vec<_> = terms.iter().map(|v| WildMatch::new(v)).collect();

    // get the text-lines that contain any of the search-terms.
    let mut result = Vec::new();
    for file in files {
        let path = PathBuf::from(".");
        let path = path.join(&file);

        let (_filter, txt) = load_file(FileFilter::Inspect, &path)?;
        let mut text_lines = Vec::new();
        for line in txt.split('\n') {
            let mut print_line = false;

            'line: for word in line.split(' ') {
                for term in &terms {
                    if term.matches(word) {
                        print_line = true;
                        break 'line;
                    }
                }
            }

            if print_line {
                text_lines.push(line.to_string());
            }
        }

        result.push((file.clone(), text_lines));
    }

    Ok(result)
}
