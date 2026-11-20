/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::ErrorKind;
use std::io::Read;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use bytes::Bytes;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::FixedResponse;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::message::HttpMessage;
use roxygen_core::source_holder::HttpSourceHolder;

/// Serves the files under one directory, keeping their contents in a cache.
///
/// Each file that a request looks up is checked on disk with one `stat`, so a request that falls
/// back past missing `.br` and `.gz` siblings costs up to three. A request can instead reuse the
/// response built for an earlier one within a revalidation interval: see
/// [`StaticFiles::with_revalidate_interval`]. A cached copy is served only while the file's
/// device, inode, size, modification time and change time all still match the copy's, and a file
/// is cached only once its change time is two seconds old, so that a rewrite within one timestamp
/// tick cannot keep a cached copy's identity. A file that is replaced or rewritten is therefore
/// read again by the next request.
///
/// Files are read whole on the calling thread, so `StaticFiles` suits files on a local disk, and
/// a file larger than the served limit is answered with an error. Only the URL path is checked:
/// symbolic links are followed wherever they point, including outside the root. It is not
/// `Sync`: create one per worker, for example in a handler factory.
///
/// With precompressed siblings enabled, a request for `app.js` whose `Accept-Encoding` allows
/// `br` is answered with `app.js.br` when that file exists, then `app.js.gz` for `gzip`, each
/// with the original's `Content-Type` and the matching `Content-Encoding`. Only compressible
/// types are looked up this way. The caller routes `GET` and `HEAD` requests here; the session
/// omits the body of a `HEAD` response.
pub struct StaticFiles {
    root: PathBuf,
    precompressed: bool,
    max_served_file: usize,
    settle: Duration,
    revalidate: Duration,
    cache: RefCell<FileCache>,
    responses: RefCell<ResponseCache>,
}

impl StaticFiles {
    /// The default bound on the memory counted for the cache of one `StaticFiles`.
    pub const DEFAULT_CACHE_LIMIT: usize = 64 << 20;
    /// The default size of the largest file that is cached. Larger files are read per request.
    pub const DEFAULT_MAX_CACHED_FILE: usize = 1 << 20;
    /// The default size of the largest file that is served.
    pub const DEFAULT_MAX_SERVED_FILE: usize = 64 << 20;

    /// Serves the files under `root` with the default limits and without precompressed siblings.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            precompressed: false,
            max_served_file: Self::DEFAULT_MAX_SERVED_FILE,
            settle: Duration::from_secs(2),
            revalidate: Duration::ZERO,
            responses: RefCell::new(ResponseCache::default()),
            cache: RefCell::new(FileCache {
                entries: HashMap::new(),
                bytes: 0,
                limit: Self::DEFAULT_CACHE_LIMIT,
                max_file: Self::DEFAULT_MAX_CACHED_FILE,
            }),
        }
    }

    /// Serves `.br` and `.gz` siblings when the request accepts them.
    pub fn with_precompressed(mut self, enabled: bool) -> Self {
        self.precompressed = enabled;
        self
    }

    /// Reuses a cached response for `interval` after its files were last checked on disk.
    ///
    /// A request for the same path whose `Accept-Encoding` allows the same precompressed
    /// codings is then answered from the response built for the first one, without a `stat`, so a
    /// file rewritten or removed within the interval can be served as it was. The default, zero,
    /// checks the files for every request. Only responses whose contents are cached are reused.
    pub fn with_revalidate_interval(mut self, interval: Duration) -> Self {
        self.revalidate = interval;
        self
    }

    /// Bounds the memory counted for the cache: each entry counts its contents, its path and a
    /// fixed overhead. Entries are evicted to stay under the bound.
    pub fn with_cache_limit(self, bytes: usize) -> Self {
        self.cache.borrow_mut().limit = bytes;
        self
    }

    /// Sets the size of the largest file that is cached.
    pub fn with_max_cached_file(self, bytes: usize) -> Self {
        self.cache.borrow_mut().max_file = bytes;
        self
    }

    /// Sets the size of the largest file that is served. Files are read whole into memory, so a
    /// larger file is answered with an error.
    pub fn with_max_served_file(mut self, bytes: usize) -> Self {
        self.max_served_file = bytes;
        self
    }

    #[cfg(test)]
    fn with_settle(mut self, settle: Duration) -> Self {
        self.settle = settle;
        self
    }

    /// The response for `path`, the percent-encoded URL path below the root without a query.
    ///
    /// A path with a `.` or `..` segment, a NUL or an invalid percent escape, or a path that does
    /// not name a regular file, is answered with 404. A file larger than the served limit and a
    /// failure to open or read the file are returned as errors.
    pub fn respond(
        &self,
        path: &[u8],
        accept_encoding: &[u8],
    ) -> Result<HttpSourceHolder, HttpError> {
        let (head, body) = self.respond_now(path, accept_encoding)?.into_parts();
        Ok(HttpSourceHolder::new(Box::new(HttpFixedSource::new(
            head,
            Some(body),
        ))))
    }

    /// The response [`Self::respond`] answers with, as a whole response that a handler can
    /// return from `HttpHandler::respond_now`.
    pub fn respond_now(
        &self,
        path: &[u8],
        accept_encoding: &[u8],
    ) -> Result<FixedResponse, HttpError> {
        if self.revalidate.is_zero() {
            let found = self.lookup(path, accept_encoding)?;
            return Ok(FixedResponse::new(
                response_head(&found)?,
                body_queue(found.body)?,
            ));
        }
        let codings = self.codings(accept_encoding);
        let now = Instant::now();
        let cached = self
            .responses
            .borrow()
            .get(path, codings, now, self.revalidate);
        if let Some((head, body)) = cached {
            return Ok(FixedResponse::from_parts(head, body_queue(body)?));
        }
        let found = self.lookup(path, accept_encoding)?;
        let response = FixedResponse::new(response_head(&found)?, body_queue(found.body.clone())?);
        if !found.cached {
            return Ok(response);
        }
        let (mut head, body) = response.into_parts();
        head.headers_mut().share_entries();
        self.responses.borrow_mut().insert(
            path,
            codings,
            CachedResponse {
                head: head.clone(),
                body: found.body,
                checked: now,
            },
        );
        Ok(FixedResponse::from_parts(head, body))
    }

    /// The precompressed codings that `accept_encoding` allows, as an index into a path's
    /// cached responses.
    fn codings(&self, accept_encoding: &[u8]) -> usize {
        if !self.precompressed {
            return 0;
        }
        usize::from(accepts(accept_encoding, b"br"))
            | usize::from(accepts(accept_encoding, b"gzip")) << 1
    }

    fn lookup(&self, path: &[u8], accept_encoding: &[u8]) -> Result<Found, HttpError> {
        let Some(relative) = relative_path(path) else {
            return Ok(Found::not_found());
        };
        let (content_type, compressible) = content_type(&relative);
        let file = self.root.join(&relative);
        let precompressed = self.precompressed && compressible;
        if precompressed {
            for (coding, suffix) in [(&b"br"[..], ".br"), (&b"gzip"[..], ".gz")] {
                if !accepts(accept_encoding, coding) {
                    continue;
                }
                let mut sibling = file.clone().into_os_string();
                sibling.push(suffix);
                if let Some((body, cached)) = self.read(Path::new(&sibling))? {
                    return Ok(Found {
                        status: 200,
                        content_type: Some(content_type),
                        content_encoding: Some(coding),
                        vary: true,
                        body,
                        cached,
                    });
                }
            }
        }
        Ok(match self.read(&file)? {
            Some((body, cached)) => Found {
                status: 200,
                content_type: Some(content_type),
                content_encoding: None,
                vary: precompressed,
                body,
                cached,
            },
            None => Found::not_found(),
        })
    }

    /// The contents of the regular file at `path` and whether they are cached, or `None` when
    /// `path` does not name a regular file.
    fn read(&self, path: &Path) -> Result<Option<(Bytes, bool)>, HttpError> {
        let key = path.as_os_str().as_bytes();
        let identity = match fs::metadata(path) {
            Ok(metadata) if metadata.is_file() => FileIdentity::of(&metadata),
            _ => {
                if self.cache.borrow_mut().remove(key) {
                    self.responses.borrow_mut().clear();
                }
                return Ok(None);
            }
        };
        if let Some(body) = self.cache.borrow().get(key, identity) {
            return Ok(Some((body, true)));
        }
        let started = SystemTime::now();
        // Cache under the opened file's identity: the file can be replaced after the `stat` above.
        let Some((file, metadata)) = open_regular(path)? else {
            return Ok(None);
        };
        let limit = self.max_served_file as u64;
        if metadata.len() > limit {
            return Err(too_large());
        }
        let mut contents = Vec::new();
        contents
            .try_reserve_exact(metadata.len() as usize)
            .map_err(internal)?;
        file.take(limit.saturating_add(1))
            .read_to_end(&mut contents)
            .map_err(internal)?;
        if contents.len() as u64 > limit {
            return Err(too_large());
        }
        let body = Bytes::from(contents);
        // Responses built from an earlier version of this file, or from one the insertion below
        // evicts, must not be reused.
        self.responses.borrow_mut().clear();
        // A file that changed size while it was read is served but not cached, and so is a file
        // changed shortly before the read: a later write in the same timestamp tick would leave
        // its identity unchanged.
        let cached = body.len() as u64 == metadata.len()
            && settled(&metadata, started, self.settle)
            && self
                .cache
                .borrow_mut()
                .insert(key, FileIdentity::of(&metadata), body.clone());
        Ok(Some((body, cached)))
    }

    #[cfg(test)]
    fn cached_bytes(&self) -> usize {
        self.cache.borrow().bytes
    }

    #[cfg(test)]
    fn cached_files(&self) -> usize {
        self.cache.borrow().entries.len()
    }
}

struct Found {
    status: u16,
    content_type: Option<&'static [u8]>,
    content_encoding: Option<&'static [u8]>,
    vary: bool,
    body: Bytes,
    cached: bool,
}

impl Found {
    fn not_found() -> Self {
        Self {
            status: 404,
            content_type: None,
            content_encoding: None,
            vary: false,
            body: Bytes::new(),
            cached: false,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    device: u64,
    inode: u64,
    size: u64,
    modified: (i64, i64),
    changed: (i64, i64),
}

impl FileIdentity {
    fn of(metadata: &fs::Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            size: metadata.size(),
            modified: (metadata.mtime(), metadata.mtime_nsec()),
            changed: (metadata.ctime(), metadata.ctime_nsec()),
        }
    }
}

struct CachedFile {
    identity: FileIdentity,
    body: Bytes,
}

struct FileCache {
    entries: HashMap<Box<[u8]>, CachedFile>,
    bytes: usize,
    limit: usize,
    max_file: usize,
}

/// The memory one cache entry is counted for beyond its key and contents: its map slot and the
/// shared buffer's header.
const ENTRY_OVERHEAD: usize = 64;

/// The memory a cache entry is counted for, so that the limit also bounds entries with short or
/// empty contents.
fn entry_cost(key: &[u8], body: usize) -> usize {
    key.len() + body + ENTRY_OVERHEAD
}

impl FileCache {
    fn get(&self, key: &[u8], identity: FileIdentity) -> Option<Bytes> {
        self.entries
            .get(key)
            .filter(|cached| cached.identity == identity)
            .map(|cached| cached.body.clone())
    }

    /// Removes the entry for `key` and returns whether there was one.
    fn remove(&mut self, key: &[u8]) -> bool {
        let Some((key, cached)) = self.entries.remove_entry(key) else {
            return false;
        };
        self.bytes -= entry_cost(&key, cached.body.len());
        true
    }

    /// Caches `body` under `key` and returns whether it fits the limits.
    fn insert(&mut self, key: &[u8], identity: FileIdentity, body: Bytes) -> bool {
        self.remove(key);
        let cost = entry_cost(key, body.len());
        if body.len() > self.max_file || cost > self.limit {
            return false;
        }
        while self.bytes + cost > self.limit {
            let Some(evicted) = self.entries.keys().next().cloned() else {
                break;
            };
            self.remove(&evicted);
        }
        self.bytes += cost;
        self.entries
            .insert(key.into(), CachedFile { identity, body });
        true
    }
}

/// A response that a request for the same path and codings can reuse.
struct CachedResponse {
    /// Carries `Content-Length` and shares its header entries with its clones.
    head: HttpMessage,
    body: Bytes,
    /// When the response's files were last checked on disk.
    checked: Instant,
}

/// The most paths with reusable responses. Inserting another path clears them all.
const MAX_RESPONSE_PATHS: usize = 1024;

/// Responses by request path, each indexed by the precompressed codings the request allows.
/// The bodies are shared with the file cache, which is why any change to it clears this one.
#[derive(Default)]
struct ResponseCache {
    entries: HashMap<Box<[u8]>, [Option<CachedResponse>; 4]>,
}

impl ResponseCache {
    fn get(
        &self,
        path: &[u8],
        codings: usize,
        now: Instant,
        interval: Duration,
    ) -> Option<(HttpMessage, Bytes)> {
        let cached = self.entries.get(path)?[codings].as_ref()?;
        (now.saturating_duration_since(cached.checked) < interval)
            .then(|| (cached.head.clone(), cached.body.clone()))
    }

    fn insert(&mut self, path: &[u8], codings: usize, response: CachedResponse) {
        if self.entries.len() >= MAX_RESPONSE_PATHS && !self.entries.contains_key(path) {
            self.entries.clear();
        }
        self.entries.entry(path.into()).or_default()[codings] = Some(response);
    }

    fn clear(&mut self) {
        self.entries.clear();
    }
}

/// The response head for `found`.
fn response_head(found: &Found) -> Result<HttpMessage, HttpError> {
    let mut head = HttpMessage::new();
    head.set_status_code(found.status).map_err(internal)?;
    head.set_status_message(HttpMessage::default_reason(found.status))
        .map_err(internal)?;
    head.set_http_version(1, 1);
    let headers = head.headers_mut();
    if let Some(content_type) = found.content_type {
        headers.add(HttpHeaderCode::ContentType, content_type);
    }
    if let Some(coding) = found.content_encoding {
        headers.add(HttpHeaderCode::ContentEncoding, coding);
    }
    if found.vary {
        headers.add(HttpHeaderCode::Vary, b"Accept-Encoding");
    }
    Ok(head)
}

fn body_queue(body: Bytes) -> Result<BufQueue, HttpError> {
    let length = body.len();
    BufQueue::from_bytes(body, length).map_err(internal)
}

/// Opens `path` and returns it with its `fstat`, or `None` when it no longer names a regular
/// file. `O_NONBLOCK` keeps the open from waiting on a FIFO that replaced the file after the
/// caller's `stat`.
fn open_regular(path: &Path) -> Result<Option<(File, fs::Metadata)>, HttpError> {
    let file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if matches!(error.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) => {
            return Ok(None);
        }
        Err(error) => return Err(internal(error)),
    };
    let metadata = file.metadata().map_err(internal)?;
    Ok(metadata.is_file().then_some((file, metadata)))
}

/// Whether the file's change time is at least `settle` before `started`.
fn settled(metadata: &fs::Metadata, started: SystemTime, settle: Duration) -> bool {
    let changed = u64::try_from(metadata.ctime())
        .ok()
        .zip(u32::try_from(metadata.ctime_nsec()).ok())
        .and_then(|(seconds, nanos)| UNIX_EPOCH.checked_add(Duration::new(seconds, nanos)));
    changed.is_some_and(|changed| {
        started
            .duration_since(changed)
            .is_ok_and(|age| age >= settle)
    })
}

fn too_large() -> HttpError {
    HttpError::with_message(
        HttpErrorCode::INTERNAL_ERROR,
        b"file is larger than the served size limit",
    )
}

fn internal(error: impl std::fmt::Display) -> HttpError {
    HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, error.to_string())
}

/// The decoded path below the root, or `None` for a path with a `.` or `..` segment, a NUL or
/// an invalid percent escape. Empty segments are skipped.
fn relative_path(path: &[u8]) -> Option<PathBuf> {
    let decoded = percent_decode(path)?;
    let mut relative = PathBuf::new();
    for segment in decoded.split(|&byte| byte == b'/') {
        match segment {
            b"" => {}
            b"." | b".." => return None,
            _ if segment.contains(&0) => return None,
            _ => relative.push(OsStr::from_bytes(segment)),
        }
    }
    (!relative.as_os_str().is_empty()).then_some(relative)
}

fn percent_decode(path: &[u8]) -> Option<Vec<u8>> {
    let mut decoded = Vec::with_capacity(path.len());
    let mut bytes = path.iter();
    while let Some(&byte) = bytes.next() {
        if byte != b'%' {
            decoded.push(byte);
            continue;
        }
        let high = (*bytes.next()? as char).to_digit(16)?;
        let low = (*bytes.next()? as char).to_digit(16)?;
        decoded.push((high * 16 + low) as u8);
    }
    Some(decoded)
}

/// The `Content-Type` for a file name's extension, and whether the type is compressible enough
/// to have precompressed siblings.
fn content_type(path: &Path) -> (&'static [u8], bool) {
    let extension = path
        .extension()
        .map(|extension| extension.as_bytes().to_ascii_lowercase())
        .unwrap_or_default();
    match extension.as_slice() {
        b"html" | b"htm" => (b"text/html", true),
        b"css" => (b"text/css", true),
        b"js" | b"mjs" => (b"application/javascript", true),
        b"json" => (b"application/json", true),
        b"svg" => (b"image/svg+xml", true),
        b"txt" => (b"text/plain", true),
        b"xml" => (b"application/xml", true),
        b"wasm" => (b"application/wasm", true),
        b"webp" => (b"image/webp", false),
        b"png" => (b"image/png", false),
        b"jpg" | b"jpeg" => (b"image/jpeg", false),
        b"gif" => (b"image/gif", false),
        b"avif" => (b"image/avif", false),
        b"ico" => (b"image/x-icon", false),
        b"woff" => (b"font/woff", false),
        b"woff2" => (b"font/woff2", false),
        b"pdf" => (b"application/pdf", false),
        b"mp4" => (b"video/mp4", false),
        b"webm" => (b"video/webm", false),
        _ => (b"application/octet-stream", false),
    }
}

/// Whether `Accept-Encoding` lists `coding` without a zero q-value.
fn accepts(accept_encoding: &[u8], coding: &[u8]) -> bool {
    accept_encoding.split(|&byte| byte == b',').any(|entry| {
        let mut parameters = entry.split(|&byte| byte == b';');
        let token = parameters.next().unwrap_or_default().trim_ascii();
        token.eq_ignore_ascii_case(coding)
            && !parameters.any(|parameter| {
                parameter
                    .trim_ascii()
                    .strip_prefix(b"q=")
                    .and_then(|q| std::str::from_utf8(q).ok()?.trim().parse::<f32>().ok())
                    .is_some_and(|q| q == 0.0)
            })
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use roxygen_core::source::HttpSource;

    use super::*;

    /// A fresh directory under the system temporary directory, removed on drop.
    struct Root(PathBuf);

    impl Root {
        fn new() -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            let path = std::env::temp_dir().join(format!(
                "roxygen-static-files-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn write(&self, name: &str, contents: &[u8]) -> PathBuf {
            let path = self.0.join(name);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            fs::write(&path, contents).unwrap();
            path
        }
    }

    impl Drop for Root {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn get(files: &StaticFiles, path: &str, accept_encoding: &str) -> Found {
        files
            .lookup(path.as_bytes(), accept_encoding.as_bytes())
            .unwrap()
    }

    #[test]
    fn serves_files_with_their_content_type() {
        let root = Root::new();
        root.write("reset.css", b"body{}");
        root.write("js/app.js", b"run()");
        let files = StaticFiles::new(&root.0);

        let found = get(&files, "reset.css", "");
        assert_eq!(found.status, 200);
        assert_eq!(found.content_type, Some(&b"text/css"[..]));
        assert_eq!(found.content_encoding, None);
        assert!(
            !found.vary,
            "without precompressed siblings the response does not vary"
        );
        assert_eq!(found.body, &b"body{}"[..]);

        let found = get(&files, "/js/%61pp.js", "");
        assert_eq!(found.content_type, Some(&b"application/javascript"[..]));
        assert_eq!(found.body, &b"run()"[..], "the path is percent-decoded");
        assert_eq!(get(&files, "missing.txt", "").status, 404);
        assert_eq!(
            get(&files, "js", "").status,
            404,
            "a directory is not served"
        );
    }

    #[test]
    fn paths_that_leave_the_root_are_not_served() {
        let root = Root::new();
        root.write("inner/secret.txt", b"secret");
        let files = StaticFiles::new(root.0.join("inner"));
        assert_eq!(get(&files, "secret.txt", "").status, 200);
        for path in [
            "",
            "/",
            "../inner/secret.txt",
            "a/../secret.txt",
            "%2e%2e/inner/secret.txt",
            "./secret.txt",
            "secret.txt%00",
            "secret.txt%zz",
            "secret.txt%2",
        ] {
            assert_eq!(get(&files, path, "").status, 404, "{path:?}");
        }
    }

    #[test]
    fn precompressed_siblings_follow_accept_encoding() {
        let root = Root::new();
        root.write("app.js", b"identity");
        root.write("app.js.br", b"brotli");
        root.write("app.js.gz", b"gzip");
        root.write("font.woff2", b"font");
        root.write("font.woff2.br", b"never served");
        let files = StaticFiles::new(&root.0).with_precompressed(true);

        let found = get(&files, "app.js", "br;q=1, gzip;q=0.8");
        assert_eq!(found.content_type, Some(&b"application/javascript"[..]));
        assert_eq!(found.content_encoding, Some(&b"br"[..]));
        assert!(found.vary);
        assert_eq!(found.body, &b"brotli"[..]);

        let found = get(&files, "app.js", "gzip, br;q=0");
        assert_eq!(found.content_encoding, Some(&b"gzip"[..]));
        assert_eq!(found.body, &b"gzip"[..]);

        let found = get(&files, "app.js", "deflate");
        assert_eq!(found.content_encoding, None);
        assert!(
            found.vary,
            "the identity response still varies by Accept-Encoding"
        );
        assert_eq!(found.body, &b"identity"[..]);

        let found = get(&files, "font.woff2", "br");
        assert_eq!(
            found.content_encoding, None,
            "the sibling of an incompressible type is not served"
        );
        assert!(!found.vary);
        assert_eq!(found.body, &b"font"[..]);

        let plain = StaticFiles::new(&root.0);
        assert_eq!(get(&plain, "app.js", "br").body, &b"identity"[..]);
    }

    #[test]
    fn a_replaced_or_rewritten_file_is_read_again() {
        let root = Root::new();
        let path = root.write("page.html", b"first");
        let files = StaticFiles::new(&root.0).with_settle(Duration::ZERO);
        assert_eq!(get(&files, "page.html", "").body, &b"first"[..]);
        assert_eq!(files.cached_files(), 1);

        // A same-size replacement through a rename has a new inode.
        let staged = root.write("staged", b"other");
        fs::rename(&staged, &path).unwrap();
        assert_eq!(get(&files, "page.html", "").body, &b"other"[..]);

        // An in-place rewrite of the same size, with the old modification time put back, is
        // still seen through its change time.
        let modified = fs::metadata(&path).unwrap().modified().unwrap();
        std::thread::sleep(Duration::from_millis(10));
        fs::write(&path, b"third").unwrap();
        File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(modified)
            .unwrap();
        assert_eq!(fs::metadata(&path).unwrap().modified().unwrap(), modified);
        assert_eq!(get(&files, "page.html", "").body, &b"third"[..]);

        fs::remove_file(&path).unwrap();
        assert_eq!(get(&files, "page.html", "").status, 404);
        assert_eq!(files.cached_files(), 0, "a deleted file leaves the cache");
        assert_eq!(files.cached_bytes(), 0);
    }

    #[test]
    fn a_recently_changed_file_is_served_but_not_cached() {
        let root = Root::new();
        let path = root.write("page.html", b"fresh");
        let files = StaticFiles::new(&root.0);
        assert_eq!(get(&files, "page.html", "").body, &b"fresh"[..]);
        assert_eq!(
            files.cached_files(),
            0,
            "a file changed within the settle time can still change without a new identity"
        );

        fs::write(&path, b"again").unwrap();
        assert_eq!(get(&files, "page.html", "").body, &b"again"[..]);
    }

    #[test]
    fn the_cache_stays_within_its_bounds() {
        let root = Root::new();
        let a = root.write("a.txt", &[b'a'; 40]);
        root.write("b.txt", &[b'b'; 40]);
        root.write("big.txt", &[b'c'; 100]);
        // "a.txt" and "b.txt" have keys of the same length, so either entry costs `entry`.
        let entry = entry_cost(a.as_os_str().as_bytes(), 40);
        let files = StaticFiles::new(&root.0)
            .with_cache_limit(entry + 20)
            .with_max_cached_file(50)
            .with_settle(Duration::ZERO);

        assert_eq!(get(&files, "big.txt", "").body.len(), 100);
        assert_eq!(
            files.cached_files(),
            0,
            "a file above the size limit is not cached"
        );
        assert_eq!(get(&files, "a.txt", "").body, &[b'a'; 40][..]);
        assert_eq!(files.cached_bytes(), entry);
        assert_eq!(get(&files, "b.txt", "").body, &[b'b'; 40][..]);
        assert_eq!(files.cached_files(), 1, "a.txt was evicted to fit b.txt");
        assert_eq!(files.cached_bytes(), entry);
        assert_eq!(get(&files, "a.txt", "").body, &[b'a'; 40][..]);
    }

    #[test]
    fn empty_files_count_against_the_cache_limit() {
        let root = Root::new();
        let first = root.write("0.txt", b"");
        for index in 1..10 {
            root.write(&format!("{index}.txt"), b"");
        }
        let entry = entry_cost(first.as_os_str().as_bytes(), 0);
        let files = StaticFiles::new(&root.0)
            .with_cache_limit(3 * entry)
            .with_settle(Duration::ZERO);
        for index in 0..10 {
            assert_eq!(get(&files, &format!("{index}.txt"), "").status, 200);
        }
        assert_eq!(
            files.cached_files(),
            3,
            "each empty file still costs its key and overhead"
        );
        assert_eq!(files.cached_bytes(), 3 * entry);
    }

    #[test]
    fn files_above_the_served_limit_are_answered_with_an_error() {
        let root = Root::new();
        root.write("small.txt", &[b's'; 50]);
        root.write("large.txt", &[b'l'; 100]);
        let files = StaticFiles::new(&root.0).with_max_served_file(50);
        assert_eq!(get(&files, "small.txt", "").body.len(), 50);
        let error = files
            .lookup(b"large.txt", b"")
            .err()
            .expect("large.txt is refused");
        assert_eq!(error.code(), HttpErrorCode::INTERNAL_ERROR);

        // A sparse file far larger than memory is refused before anything is allocated for it.
        File::create(root.0.join("sparse.bin"))
            .unwrap()
            .set_len(1 << 36)
            .unwrap();
        let files = StaticFiles::new(&root.0);
        assert!(files.lookup(b"sparse.bin", b"").is_err());
    }

    #[test]
    fn opening_a_fifo_does_not_block() {
        let root = Root::new();
        let fifo = root.0.join("pipe.txt");
        let name = std::ffi::CString::new(fifo.as_os_str().as_bytes()).unwrap();
        // SAFETY: `name` is a valid NUL-terminated path that outlives the call.
        assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
        assert!(
            open_regular(&fifo).unwrap().is_none(),
            "a FIFO is opened without waiting for a writer and is not a regular file"
        );
        assert_eq!(get(&StaticFiles::new(&root.0), "pipe.txt", "").status, 404);
    }

    /// The `Content-Encoding` and the whole body of the response for `path`.
    fn fetch(
        runtime: &tokio::runtime::Runtime,
        files: &StaticFiles,
        path: &str,
        accept_encoding: &str,
    ) -> (Vec<u8>, Vec<u8>) {
        runtime.block_on(async {
            let mut source = files
                .respond(path.as_bytes(), accept_encoding.as_bytes())
                .unwrap();
            let head = source.read_header_event().await.unwrap();
            let coding = head
                .headers()
                .headers()
                .get_single_or_empty(HttpHeaderCode::ContentEncoding)
                .to_vec();
            let mut body = Vec::new();
            loop {
                let event = source.read_body_event(u32::MAX).await.unwrap();
                if let Some(data) = event.as_body() {
                    body.extend_from_slice(&data.coalesce(usize::MAX).unwrap());
                }
                if event.eom() {
                    break;
                }
            }
            (coding, body)
        })
    }

    fn current_thread() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
    }

    #[test]
    fn revalidate_interval_reuses_the_response_until_it_ends() {
        let root = Root::new();
        root.write("note.txt", b"first");
        let runtime = current_thread();
        let reused = StaticFiles::new(&root.0)
            .with_settle(Duration::ZERO)
            .with_revalidate_interval(Duration::from_secs(3600));
        let checked = StaticFiles::new(&root.0).with_settle(Duration::ZERO);
        let expiring = StaticFiles::new(&root.0)
            .with_settle(Duration::ZERO)
            .with_revalidate_interval(Duration::from_millis(20));
        for files in [&reused, &checked, &expiring] {
            assert_eq!(fetch(&runtime, files, "note.txt", "").1, b"first");
        }

        root.write("note.txt", b"second version");
        assert_eq!(
            fetch(&runtime, &reused, "note.txt", "").1,
            b"first",
            "within the interval the earlier response is reused without checking the file"
        );
        assert_eq!(
            fetch(&runtime, &checked, "note.txt", "").1,
            b"second version",
            "without an interval every request checks the file"
        );
        std::thread::sleep(Duration::from_millis(40));
        assert_eq!(
            fetch(&runtime, &expiring, "note.txt", "").1,
            b"second version",
            "after the interval the file is checked again"
        );
    }

    #[test]
    fn revalidate_interval_keeps_one_response_per_accepted_codings() {
        let root = Root::new();
        root.write("app.js", b"plain");
        root.write("app.js.br", b"brotli");
        root.write("app.js.gz", b"gzipped");
        let runtime = current_thread();
        let files = StaticFiles::new(&root.0)
            .with_precompressed(true)
            .with_settle(Duration::ZERO)
            .with_revalidate_interval(Duration::from_secs(3600));
        for _ in 0..2 {
            assert_eq!(
                fetch(&runtime, &files, "app.js", "br;q=1, gzip;q=0.8"),
                (b"br".to_vec(), b"brotli".to_vec())
            );
            assert_eq!(
                fetch(&runtime, &files, "app.js", "gzip"),
                (b"gzip".to_vec(), b"gzipped".to_vec())
            );
            assert_eq!(
                fetch(&runtime, &files, "app.js", ""),
                (Vec::new(), b"plain".to_vec())
            );
        }
        let missing = runtime.block_on(async {
            let mut source = files.respond(b"none.js", b"br").unwrap();
            source.read_header_event().await.unwrap()
        });
        assert_eq!(missing.headers().response_status(), Some(404));
    }

    type Response = (Option<u16>, Vec<(Vec<u8>, Vec<u8>)>, Vec<u8>);

    fn headers_of(message: &HttpMessage) -> Vec<(Vec<u8>, Vec<u8>)> {
        message
            .headers()
            .iter()
            .map(|(_, name, value)| (name.to_vec(), value.to_vec()))
            .collect()
    }

    /// The status, headers and body of the response that `respond_now` returns.
    fn fixed(files: &StaticFiles, path: &str, accept_encoding: &str) -> Response {
        let (head, body) = files
            .respond_now(path.as_bytes(), accept_encoding.as_bytes())
            .unwrap()
            .into_parts();
        let body = body.coalesce(usize::MAX).unwrap().to_vec();
        (head.response_status(), headers_of(&head), body)
    }

    /// The status, headers and body that the source from `respond` produces.
    fn streamed(
        runtime: &tokio::runtime::Runtime,
        files: &StaticFiles,
        path: &str,
        accept_encoding: &str,
    ) -> Response {
        runtime.block_on(async {
            let mut source = files
                .respond(path.as_bytes(), accept_encoding.as_bytes())
                .unwrap();
            let head = source.read_header_event().await.unwrap();
            let mut body = Vec::new();
            let mut eom = head.eom();
            while !eom {
                let event = source.read_body_event(u32::MAX).await.unwrap();
                if let Some(data) = event.as_body() {
                    body.extend_from_slice(&data.coalesce(usize::MAX).unwrap());
                }
                eom = event.eom();
            }
            (
                head.headers().response_status(),
                headers_of(head.headers()),
                body,
            )
        })
    }

    #[test]
    fn respond_now_answers_as_respond() {
        let root = Root::new();
        root.write("app.js", b"plain");
        root.write("app.js.br", b"brotli");
        root.write("app.js.gz", b"gzipped");
        root.write("logo.png", b"png");
        let runtime = current_thread();
        for interval in [Duration::ZERO, Duration::from_secs(3600)] {
            let files = StaticFiles::new(&root.0)
                .with_precompressed(true)
                .with_settle(Duration::ZERO)
                .with_revalidate_interval(interval);
            // With an interval, the second round reuses the responses of the first.
            for _ in 0..2 {
                for (path, accept_encoding) in [
                    ("app.js", "br, gzip"),
                    ("app.js", "gzip"),
                    ("app.js", ""),
                    ("logo.png", "br"),
                    ("missing.js", "br"),
                ] {
                    let response = fixed(&files, path, accept_encoding);
                    assert_eq!(
                        response,
                        streamed(&runtime, &files, path, accept_encoding),
                        "{path} {accept_encoding:?} {interval:?}"
                    );
                    let (_, headers, body) = response;
                    assert_eq!(
                        headers.last(),
                        Some(&(
                            b"Content-Length".to_vec(),
                            body.len().to_string().into_bytes()
                        )),
                        "{path} {accept_encoding:?}: the head carries the body's length last"
                    );
                }
            }
        }
        let (_, headers, body) = fixed(
            &StaticFiles::new(&root.0).with_precompressed(true),
            "app.js",
            "gzip",
        );
        assert_eq!(body, b"gzipped");
        assert_eq!(
            headers,
            [
                (b"Content-Type".to_vec(), b"application/javascript".to_vec()),
                (b"Content-Encoding".to_vec(), b"gzip".to_vec()),
                (b"Vary".to_vec(), b"Accept-Encoding".to_vec()),
                (b"Content-Length".to_vec(), b"7".to_vec()),
            ]
        );
    }

    #[test]
    fn respond_now_reuses_a_response_within_the_revalidate_interval() {
        let root = Root::new();
        root.write("note.txt", b"first");
        let reused = StaticFiles::new(&root.0)
            .with_settle(Duration::ZERO)
            .with_revalidate_interval(Duration::from_secs(3600));
        let checked = StaticFiles::new(&root.0).with_settle(Duration::ZERO);
        for files in [&reused, &checked] {
            assert_eq!(fixed(files, "note.txt", "").2, b"first");
        }

        root.write("note.txt", b"second version");
        let (_, headers, body) = fixed(&reused, "note.txt", "");
        assert_eq!(body, b"first", "the earlier response is reused");
        assert_eq!(
            headers.last(),
            Some(&(b"Content-Length".to_vec(), b"5".to_vec()))
        );
        let (_, headers, body) = fixed(&checked, "note.txt", "");
        assert_eq!(
            body, b"second version",
            "without an interval the file is checked"
        );
        assert_eq!(
            headers.last(),
            Some(&(b"Content-Length".to_vec(), b"14".to_vec()))
        );
    }

    #[test]
    fn respond_builds_the_response_message() {
        let root = Root::new();
        root.write("index.html", b"<p>hi</p>");
        root.write("index.html.gz", b"zipped");
        let files = StaticFiles::new(&root.0).with_precompressed(true);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut source = files.respond(b"index.html", b"gzip").unwrap();
            let head = source.read_header_event().await.unwrap();
            let message = head.headers();
            assert_eq!(message.response_status(), Some(200));
            let headers = message.headers();
            assert_eq!(
                headers.get_single_or_empty(HttpHeaderCode::ContentType),
                b"text/html"
            );
            assert_eq!(
                headers.get_single_or_empty(HttpHeaderCode::ContentEncoding),
                b"gzip"
            );
            assert_eq!(
                headers.get_single_or_empty(HttpHeaderCode::Vary),
                b"Accept-Encoding"
            );

            let mut missing = files.respond(b"none.html", b"").unwrap();
            let head = missing.read_header_event().await.unwrap();
            assert_eq!(head.headers().response_status(), Some(404));
        });
    }
}
