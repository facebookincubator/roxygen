//! A curl-compatible command-line client backed by Roxygen.

mod args;
mod transfer;

use std::ffi::OsString;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use roxygen_runtime::tokio_host::ManagedWorker;

use crate::args::Options;
use crate::args::OutputTarget;
use crate::transfer::ExchangeTrace;
use crate::transfer::TransferResult;

const HELP: &str = "Usage: curl [options...] <url>...

Roxygen curl supports:
  -X, --request <method>          HTTP method (CONNECT is unsupported)
  -H, --header <header>           Add or remove a request header
  -d, --data <data>               URL-encoded request data
      --data-raw <data>           Data without special @ handling
      --data-binary <data>        Binary request data
      --data-urlencode <data>     URL-encode request data
      --json <data>               JSON request data and headers
  -F, --form <name=content>       Multipart form data
  -T, --upload-file <file>        Upload a file with PUT
  -G, --get                       Put data in the query string
  -o, --output <file>             Write body to a file
  -O, --remote-name               Use the remote path basename
  -L, --location                  Follow redirects
  -i, --include                   Include response headers
  -I, --head                      Send HEAD and show headers
  -s, --silent                    Suppress progress and errors
  -S, --show-error                Show errors with --silent
  -v, --verbose                   Trace headers to stderr
  -f, --fail                      Fail on HTTP status >= 400
  -u, --user <user:password>      HTTP Basic credentials
  -k, --insecure                  Disable TLS verification
  -A, --user-agent <name>         Set User-Agent
  -e, --referer <url>             Set Referer
  -m, --max-time <seconds>        Limit the whole transfer
      --connect-timeout <seconds> Limit connection establishment
      --retry <num>               Retry transient failures
      --retry-delay <seconds>     Delay between retries
      --retry-max-time <seconds>  Limit retry time
  -b, --cookie <data|file>        Send cookies
  -c, --cookie-jar <file>         Save received cookies
  -w, --write-out <format>        Print transfer metadata
  -D, --dump-header <file>        Save response headers
      --trace <file>              Write a hexadecimal trace
      --trace-ascii <file>        Write an ASCII trace
  -#, --progress-bar              Use a compact progress bar
      --create-dirs               Create output parent directories
  -x, --proxy <url>               Use an HTTP proxy (no CONNECT)
      --proxy-user <user:pass>    Proxy Basic credentials
      --noproxy <hosts>           Bypass proxy for matching hosts
      --compressed                Request and decode compressed data
      --no-keepalive              Disable TCP keepalive
      --resolve <host:port:addr>  Override name resolution
      --dns-servers <addresses>   Use specified DNS servers
  -4, --ipv4 / -6, --ipv6        Select an address family
      --cacert <file>             Use a CA certificate bundle
      --cert <file> --key <file>  Use a TLS client identity
      --http1.1 / --http2         Select HTTP protocol
      --tlsv1.2 / --tlsv1.3       Select the minimum TLS version
  -r, --range <range>             Request a byte range
  -C, --continue-at <offset>      Resume an output file (- for auto)
  -Z, --parallel                  Run transfers concurrently
      --limit-rate <speed>        Limit output rate
  -K, --config <file>             Read options from a file
  -q, --disable                   Do not read the default curlrc
  -n, --netrc                     Read credentials from ~/.netrc
  -V, --version                   Show version
  -h, --help                      Show this help

Boolean long options accept a --no- counterpart. HTTP/3 and HTTPS proxy
tunnels are outside Roxygen's current HTTP/1.1 and HTTP/2 scope.
";

#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error("{0}")]
    Usage(String),
    #[error("{0}")]
    Unsupported(String),
    #[error("failed to read {path}: {source}")]
    Input {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to read stdin: {0}")]
    Stdin(#[source] io::Error),
    #[error("failed to write {path}: {source}")]
    Output {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to initialize the async runtime: {0}")]
    Runtime(#[source] io::Error),
    #[error(transparent)]
    Worker(#[from] roxygen_runtime::tokio_host::ManagedWorkerError),
    #[error(transparent)]
    Submit(#[from] roxygen_runtime::worker::SubmitError),
    #[error(transparent)]
    WorkerCall(#[from] roxygen_runtime::worker::WorkerCallError),
}

/// Runs the Roxygen curl frontend using process arguments and standard I/O.
pub fn run_from_env() -> i32 {
    run_with_io(
        std::env::args_os().skip(1),
        &mut io::stdin().lock(),
        &mut io::stdout().lock(),
        &mut io::stderr().lock(),
    )
}

/// Runs the frontend with injectable streams for tests and embedders.
pub fn run_with_io(
    arguments: impl IntoIterator<Item = OsString>,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> i32 {
    match try_run(arguments, stdin, stdout, stderr) {
        Ok(code) => code,
        Err(error) => {
            let _ = writeln!(stderr, "curl: {error}");
            match error {
                CliError::Usage(_) | CliError::Unsupported(_) => 2,
                CliError::Input { .. } | CliError::Stdin(_) => 26,
                CliError::Output { .. } => 23,
                CliError::Runtime(_)
                | CliError::Worker(_)
                | CliError::Submit(_)
                | CliError::WorkerCall(_) => 1,
            }
        }
    }
}

fn try_run(
    arguments: impl IntoIterator<Item = OsString>,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<i32, CliError> {
    let options = args::parse(arguments, stdin)?;
    if options.help {
        stdout.write_all(HELP.as_bytes()).map_err(output_stdout)?;
        return Ok(0);
    }
    if options.version {
        stdout
            .write_all(b"curl 8.10.1-roxygen (HTTP/1.1 HTTP/2)\n")
            .map_err(output_stdout)?;
        return Ok(0);
    }

    let environment = transfer::prepare_environment(&options)?;
    let worker = ManagedWorker::new(options.transfers.len().max(8))?;
    let call = worker.handle().call_local({
        let options = options.clone();
        move |context| transfer::execute(context, options, environment)
    })?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(CliError::Runtime)?;
    let network = runtime.block_on(call);
    let shutdown = worker.shutdown();
    let network = network?;
    shutdown?;

    let mut dump = open_auxiliary(options.dump_header.as_deref())?;
    let mut trace = open_auxiliary(options.trace.as_deref())?;
    let mut trace_ascii = open_auxiliary(options.trace_ascii.as_deref())?;
    let mut exit_code = 0;
    for result in network.results {
        match result {
            Ok(result) => {
                match emit_result(
                    &options,
                    result,
                    stdout,
                    stderr,
                    dump.as_mut(),
                    trace.as_mut(),
                    trace_ascii.as_mut(),
                ) {
                    Ok(true) => exit_code = 22,
                    Ok(false) => {}
                    Err(error) => {
                        if !options.silent || options.show_error {
                            let _ = writeln!(stderr, "curl: {error}");
                        }
                        exit_code = match error {
                            CliError::Output { .. } => 23,
                            _ => 1,
                        };
                    }
                }
            }
            Err(error) => {
                if !options.silent || options.show_error {
                    writeln!(stderr, "curl: ({}) {error}", error.exit_code())
                        .map_err(output_stderr)?;
                }
                exit_code = error.exit_code();
            }
        }
    }
    if let Some(path) = &options.cookie_jar {
        write_cookie_jar(path, &network.cookies.to_netscape(), stdout)?;
    }
    Ok(exit_code)
}

fn emit_result(
    options: &Options,
    result: TransferResult,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
    dump: Option<&mut AuxiliaryOutput>,
    trace: Option<&mut AuxiliaryOutput>,
    trace_ascii: Option<&mut AuxiliaryOutput>,
) -> Result<bool, CliError> {
    if options.verbose {
        emit_verbose(&result, stderr).map_err(output_stderr)?;
    }
    if let Some(output) = trace {
        let path = output.path();
        for exchange in &result.traces {
            with_auxiliary_writer(output, stdout, |writer| emit_trace(exchange, writer, false))
                .map_err(|source| output_error(path.clone(), source))?;
        }
    }
    if let Some(output) = trace_ascii {
        let path = output.path();
        for exchange in &result.traces {
            with_auxiliary_writer(output, stdout, |writer| emit_trace(exchange, writer, true))
                .map_err(|source| output_error(path.clone(), source))?;
        }
    }
    if let Some(output) = dump {
        let path = output.path();
        for headers in &result.response_headers {
            with_auxiliary_writer(output, stdout, |writer| writer.write_all(headers))
                .map_err(|source| output_error(path.clone(), source))?;
        }
    }

    let failed = options.fail && result.status >= 400;
    let transfer = &options.transfers[result.index];
    let mut output = open_transfer_output(transfer, options.create_dirs, stdout)?;
    if options.include || options.head {
        result
            .response_headers
            .iter()
            .try_for_each(|headers| output.writer().write_all(headers))
            .map_err(|source| output_error(output.path(), source))?;
    }
    if !options.head && !failed {
        output
            .writer()
            .write_all(&result.body)
            .map_err(|source| output_error(output.path(), source))?;
    }
    output
        .writer()
        .flush()
        .map_err(|source| output_error(output.path(), source))?;

    if !options.silent {
        emit_progress(options, &result, stderr).map_err(output_stderr)?;
    }
    if let Some(format) = &options.write_out {
        let formatted = format_write_out(format, &result);
        stdout
            .write_all(formatted.as_bytes())
            .map_err(output_stdout)?;
    }
    if failed {
        if !options.silent || options.show_error {
            writeln!(
                stderr,
                "curl: (22) The requested URL returned error: {}",
                result.status
            )
            .map_err(output_stderr)?;
        }
        return Ok(true);
    }
    Ok(false)
}

enum AuxiliaryOutput {
    Stdout,
    File { path: PathBuf, file: File },
}

impl AuxiliaryOutput {
    fn path(&self) -> PathBuf {
        match self {
            Self::Stdout => PathBuf::from("standard output"),
            Self::File { path, .. } => path.clone(),
        }
    }
}

fn with_auxiliary_writer<T>(
    output: &mut AuxiliaryOutput,
    stdout: &mut dyn Write,
    operation: impl FnOnce(&mut dyn Write) -> io::Result<T>,
) -> io::Result<T> {
    match output {
        AuxiliaryOutput::Stdout => operation(stdout),
        AuxiliaryOutput::File { file, .. } => operation(file),
    }
}

enum TransferOutput<'a> {
    Stdout(&'a mut dyn Write),
    File { path: PathBuf, file: File },
}

impl TransferOutput<'_> {
    fn writer(&mut self) -> &mut dyn Write {
        match self {
            Self::Stdout(writer) => *writer,
            Self::File { file, .. } => file,
        }
    }

    fn path(&self) -> PathBuf {
        match self {
            Self::Stdout(_) => PathBuf::from("standard output"),
            Self::File { path, .. } => path.clone(),
        }
    }
}

fn open_auxiliary(path: Option<&Path>) -> Result<Option<AuxiliaryOutput>, CliError> {
    let Some(path) = path else {
        return Ok(None);
    };
    if path == Path::new("-") {
        return Ok(Some(AuxiliaryOutput::Stdout));
    }
    let file = File::create(path).map_err(|source| CliError::Output {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(Some(AuxiliaryOutput::File {
        path: path.to_path_buf(),
        file,
    }))
}

fn open_transfer_output<'a>(
    transfer: &args::TransferSpec,
    create_dirs: bool,
    stdout: &'a mut dyn Write,
) -> Result<TransferOutput<'a>, CliError> {
    let OutputTarget::File(path) = &transfer.output else {
        return Ok(TransferOutput::Stdout(stdout));
    };
    if create_dirs
        && let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).map_err(|source| CliError::Output {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    let mut options = OpenOptions::new();
    options.create(true).write(true);
    if transfer.resume_offset.is_none_or(|offset| offset == 0) {
        options.truncate(true);
    }
    let mut file = options.open(path).map_err(|source| CliError::Output {
        path: path.clone(),
        source,
    })?;
    if let Some(offset) = transfer.resume_offset {
        file.seek(SeekFrom::Start(offset))
            .map_err(|source| CliError::Output {
                path: path.clone(),
                source,
            })?;
    }
    Ok(TransferOutput::File {
        path: path.clone(),
        file,
    })
}

fn write_cookie_jar(path: &Path, contents: &str, stdout: &mut dyn Write) -> Result<(), CliError> {
    if path == Path::new("-") {
        return stdout.write_all(contents.as_bytes()).map_err(output_stdout);
    }
    fs::write(path, contents).map_err(|source| CliError::Output {
        path: path.to_path_buf(),
        source,
    })
}

fn emit_verbose(result: &TransferResult, stderr: &mut dyn Write) -> io::Result<()> {
    writeln!(stderr, "* Connected for {}", result.effective_url)?;
    for exchange in &result.traces {
        prefixed_lines(stderr, b"> ", &exchange.request_headers)?;
        prefixed_lines(stderr, b"< ", &exchange.response_headers)?;
    }
    Ok(())
}

fn prefixed_lines(output: &mut dyn Write, prefix: &[u8], bytes: &[u8]) -> io::Result<()> {
    bytes
        .split_inclusive(|byte| *byte == b'\n')
        .try_for_each(|line| {
            output.write_all(prefix)?;
            output.write_all(line)
        })
}

fn emit_trace(
    exchange: &ExchangeTrace,
    output: &mut dyn Write,
    ascii_only: bool,
) -> io::Result<()> {
    trace_block(
        output,
        b"=> Send header",
        &exchange.request_headers,
        ascii_only,
    )?;
    if !exchange.request_body.is_empty() {
        trace_block(output, b"=> Send data", &exchange.request_body, ascii_only)?;
    }
    trace_block(
        output,
        b"<= Recv header",
        &exchange.response_headers,
        ascii_only,
    )?;
    if !exchange.response_body.is_empty() {
        trace_block(output, b"<= Recv data", &exchange.response_body, ascii_only)?;
    }
    Ok(())
}

fn trace_block(
    output: &mut dyn Write,
    label: &[u8],
    bytes: &[u8],
    ascii_only: bool,
) -> io::Result<()> {
    output.write_all(label)?;
    writeln!(output, ", {} bytes", bytes.len())?;
    for (offset, chunk) in bytes.chunks(16).enumerate() {
        write!(output, "{:04x}: ", offset * 16)?;
        if !ascii_only {
            for byte in chunk {
                write!(output, "{byte:02x} ")?;
            }
            for _ in chunk.len()..16 {
                output.write_all(b"   ")?;
            }
        }
        for byte in chunk {
            output.write_all(&[if byte.is_ascii_graphic() || *byte == b' ' {
                *byte
            } else {
                b'.'
            }])?;
        }
        output.write_all(b"\n")?;
    }
    Ok(())
}

fn emit_progress(
    options: &Options,
    result: &TransferResult,
    stderr: &mut dyn Write,
) -> io::Result<()> {
    if options.progress_bar {
        writeln!(
            stderr,
            "######################################################################## 100.0%"
        )
    } else {
        writeln!(
            stderr,
            "  % Total    % Received  Average Speed   Time\n  100 {}  100 {}  {}  {:.2}s",
            result.downloaded,
            result.downloaded,
            transfer_speed(result),
            result.time_total.as_secs_f64()
        )
    }
}

fn format_write_out(format: &str, result: &TransferResult) -> String {
    let mut output = String::new();
    let mut characters = format.chars().peekable();
    while let Some(character) = characters.next() {
        match character {
            '\\' => output.push(match characters.next() {
                Some('n') => '\n',
                Some('r') => '\r',
                Some('t') => '\t',
                Some('\\') => '\\',
                Some(other) => other,
                None => '\\',
            }),
            '%' if characters.peek() == Some(&'%') => {
                characters.next();
                output.push('%');
            }
            '%' if characters.peek() == Some(&'{') => {
                characters.next();
                let name = characters
                    .by_ref()
                    .take_while(|next| *next != '}')
                    .collect::<String>();
                match name.as_str() {
                    "http_code" | "response_code" => {
                        output.push_str(&format!("{:03}", result.status))
                    }
                    "time_total" => {
                        output.push_str(&format!("{:.6}", result.time_total.as_secs_f64()))
                    }
                    "time_connect" => {
                        output.push_str(&format!("{:.6}", result.time_connect.as_secs_f64()))
                    }
                    "size_download" => output.push_str(&result.downloaded.to_string()),
                    "speed_download" => output.push_str(&transfer_speed(result).to_string()),
                    "redirect_url" => output.push_str(&result.redirect_url),
                    "url" => output.push_str(&result.original_url),
                    "url_effective" => output.push_str(&result.effective_url),
                    _ => {
                        output.push_str("%{");
                        output.push_str(&name);
                        output.push('}');
                    }
                }
            }
            other => output.push(other),
        }
    }
    output
}

fn transfer_speed(result: &TransferResult) -> u64 {
    let seconds = result.time_total.as_secs_f64();
    if seconds == 0.0 {
        return result.downloaded as u64;
    }
    (result.downloaded as f64 / seconds) as u64
}

fn output_error(path: PathBuf, source: io::Error) -> CliError {
    CliError::Output { path, source }
}

fn output_stdout(source: io::Error) -> CliError {
    output_error(PathBuf::from("standard output"), source)
}

fn output_stderr(source: io::Error) -> CliError {
    output_error(PathBuf::from("standard error"), source)
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::io::Cursor;
    use std::io::Read;
    use std::io::Write;
    use std::net::TcpListener;
    use std::net::TcpStream;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use bytes::Bytes;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use roxygen_core::buf_queue::BufQueue;
    use roxygen_core::fixed_source::HttpFixedSource;
    use roxygen_core::handler::HandlerSessionContext;
    use roxygen_core::handler::HttpHandler;
    use roxygen_core::handler::HttpHandlerRef;
    use roxygen_core::handler::LocalHandlerFuture;
    use roxygen_core::http::CodecProtocol;
    use roxygen_core::source::HttpSource;
    use roxygen_core::source_holder::HttpSourceHolder;
    use roxygen_server::scoped_server::ScopedHttpServer;
    use roxygen_server::server::HandlerFactory;
    use roxygen_server::server::ServerConfig;
    use roxygen_server::server::StaticTlsCredentials;
    use roxygen_transport::tls::TlsServerOptions;

    use super::*;

    struct LocalServer {
        url: String,
        requests: mpsc::Receiver<Vec<u8>>,
        thread: Option<thread::JoinHandle<()>>,
    }

    impl LocalServer {
        fn start(responses: Vec<Vec<u8>>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("loopback should bind");
            let address = listener.local_addr().expect("listener has an address");
            let (request_tx, requests) = mpsc::channel();
            let thread = thread::spawn(move || {
                for response in responses {
                    let (mut socket, _) = listener.accept().expect("client should connect");
                    let request = read_request(&mut socket).expect("client should send a request");
                    request_tx
                        .send(request)
                        .expect("request receiver should remain");
                    let header_end = response
                        .windows(4)
                        .position(|window| window == b"\r\n\r\n")
                        .expect("fixture response should have complete headers")
                        + 2;
                    socket
                        .write_all(&response[..header_end])
                        .expect("response headers should write");
                    socket
                        .write_all(b"Connection: close\r\n")
                        .expect("connection policy should write");
                    socket
                        .write_all(&response[header_end..])
                        .expect("response body should write");
                }
            });
            Self {
                url: format!("http://{address}"),
                requests,
                thread: Some(thread),
            }
        }

        fn start_persistent(responses: Vec<Vec<u8>>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("loopback should bind");
            let address = listener.local_addr().expect("listener has an address");
            let (request_tx, requests) = mpsc::channel();
            let thread = thread::spawn(move || {
                let (mut socket, _) = listener.accept().expect("client should connect");
                for response in responses {
                    let Some(request) = read_request(&mut socket) else {
                        return;
                    };
                    request_tx
                        .send(request)
                        .expect("request receiver should remain");
                    socket.write_all(&response).expect("response should write");
                }
            });
            Self {
                url: format!("http://{address}"),
                requests,
                thread: Some(thread),
            }
        }

        fn finish(mut self) -> Vec<Vec<u8>> {
            self.thread
                .take()
                .expect("server thread exists")
                .join()
                .expect("server should not panic");
            self.requests.try_iter().collect()
        }
    }

    fn read_request(socket: &mut TcpStream) -> Option<Vec<u8>> {
        let mut request = Vec::new();
        let mut buffer = [0_u8; 4096];
        loop {
            let read = socket.read(&mut buffer).expect("request should read");
            if read == 0 {
                return None;
            }
            request.extend_from_slice(&buffer[..read]);
            let header_end = request
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|index| index + 4);
            let Some(header_end) = header_end else {
                continue;
            };
            let headers = String::from_utf8_lossy(&request[..header_end]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    line.strip_prefix("Content-Length: ")
                        .and_then(|value| value.trim().parse::<usize>().ok())
                })
                .unwrap_or(0);
            if request.len() >= header_end + content_length {
                return Some(request);
            }
        }
    }

    struct FixedHandler;

    impl HttpHandler for FixedHandler {
        fn handle_request(
            &self,
            _context: HandlerSessionContext,
            mut request: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            Box::pin(async move {
                let header = request.read_header_event().await?;
                if !header.eom() {
                    loop {
                        let event = request.read_body_event(u32::MAX).await?;
                        if event.eom() {
                            break;
                        }
                    }
                }
                Ok(HttpSourceHolder::new(Box::new(
                    HttpFixedSource::make_fixed_response(
                        200,
                        Some(
                            BufQueue::from_bytes(Bytes::from_static(b"roxygen-h2"), 10)
                                .expect("test body is bounded"),
                        ),
                    )
                    .expect("test response is valid"),
                )))
            })
        }
    }

    fn handler_factory() -> HandlerFactory {
        Arc::new(|_| Rc::new(FixedHandler) as HttpHandlerRef)
    }

    fn run(arguments: &[String]) -> (i32, Vec<u8>, Vec<u8>) {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let code = run_with_io(
            arguments.iter().map(OsString::from),
            &mut Cursor::new(Vec::new()),
            &mut stdout,
            &mut stderr,
        );
        (code, stdout, stderr)
    }

    fn test_directory(name: &str) -> PathBuf {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        std::env::temp_dir().join(format!(
            "roxygen-curl-{name}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[test]
    fn posts_to_a_live_loopback_server_with_curl_headers() {
        let server = LocalServer::start(vec![
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nSet-Cookie: sid=abc; Path=/\r\n\r\nOK"
                .to_vec(),
        ]);
        let (code, stdout, stderr) = run(&[
            "-q".to_owned(),
            "-sS".to_owned(),
            "-H".to_owned(),
            "X-Test: present".to_owned(),
            "-d".to_owned(),
            "a=one".to_owned(),
            "-d".to_owned(),
            "b=two".to_owned(),
            format!("{}/submit", server.url),
        ]);
        assert_eq!(
            code,
            0,
            "the loopback POST should succeed: {}",
            String::from_utf8_lossy(&stderr)
        );
        assert_eq!(stdout, b"OK", "the response body should reach stdout");
        let requests = server.finish();
        let request = String::from_utf8_lossy(&requests[0]);
        assert!(request.starts_with("POST /submit HTTP/1.1\r\n"));
        assert!(request.contains("X-Test: present\r\n"));
        assert!(
            !request.contains("Connection: keep-alive\r\n"),
            "HTTP/1.1 persistence should remain implicit"
        );
        assert!(request.ends_with("a=one&b=two"));
    }

    #[test]
    fn follows_a_redirect_on_live_loopback_connections() {
        let server = LocalServer::start(vec![
            b"HTTP/1.1 302 Found\r\nContent-Length: 0\r\nLocation: /final\r\n\r\n".to_vec(),
            b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfinal".to_vec(),
        ]);
        let (code, stdout, stderr) = run(&[
            "-q".to_owned(),
            "-sS".to_owned(),
            "-L".to_owned(),
            format!("{}/start", server.url),
        ]);
        assert_eq!(
            code,
            0,
            "the redirected request should succeed: {}",
            String::from_utf8_lossy(&stderr)
        );
        assert_eq!(
            stdout, b"final",
            "only the final response body should be emitted"
        );
        let requests = server.finish();
        assert_eq!(
            requests.len(),
            2,
            "the redirect should open a second connection"
        );
        assert!(String::from_utf8_lossy(&requests[1]).starts_with("GET /final HTTP/1.1\r\n"));
    }

    #[test]
    fn fail_suppresses_the_body_and_returns_curl_exit_code_22() {
        let server = LocalServer::start(vec![
            b"HTTP/1.1 404 Not Found\r\nContent-Length: 7\r\n\r\nmissing".to_vec(),
        ]);
        let (code, stdout, stderr) = run(&[
            "-q".to_owned(),
            "-sS".to_owned(),
            "-f".to_owned(),
            server.url.clone(),
        ]);
        assert_eq!(code, 22, "--fail should use curl's HTTP error code");
        assert!(
            stdout.is_empty(),
            "--fail should suppress the response body"
        );
        assert!(String::from_utf8_lossy(&stderr).contains("(22)"));
        server.finish();
    }

    #[test]
    fn formats_required_write_out_variables() {
        let result = TransferResult {
            index: 0,
            original_url: "http://example.test/".to_owned(),
            effective_url: "http://example.test/final".to_owned(),
            response_headers: Vec::new(),
            body: b"body".to_vec(),
            status: 201,
            redirect_url: String::new(),
            time_total: Duration::from_secs(2),
            time_connect: Duration::from_millis(250),
            downloaded: 4,
            traces: Vec::new(),
            cookies: Default::default(),
        };
        assert_eq!(
            format_write_out(
                "%{http_code} %{size_download} %{speed_download} %{url_effective}\\n",
                &result
            ),
            "201 4 2 http://example.test/final\n"
        );
    }

    #[test]
    fn resolve_override_reaches_a_live_loopback_server() {
        let server = LocalServer::start(vec![
            b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\nresolved".to_vec(),
        ]);
        let parsed = url::Url::parse(&server.url).expect("server URL should parse");
        let port = parsed.port().expect("server URL has a port");
        let (code, stdout, stderr) = run(&[
            "-q".to_owned(),
            "-sS".to_owned(),
            "--resolve".to_owned(),
            format!("example.test:{port}:127.0.0.1"),
            format!("http://example.test:{port}/resolved"),
        ]);
        assert_eq!(
            code,
            0,
            "the resolved request should succeed: {}",
            String::from_utf8_lossy(&stderr)
        );
        assert_eq!(stdout, b"resolved");
        let requests = server.finish();
        assert!(String::from_utf8_lossy(&requests[0]).contains("Host: example.test:"));
    }

    #[test]
    fn plaintext_proxy_receives_an_absolute_uri_and_credentials() {
        let server = LocalServer::start(vec![
            b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nproxy".to_vec(),
        ]);
        let (code, stdout, stderr) = run(&[
            "-q".to_owned(),
            "-sS".to_owned(),
            "-x".to_owned(),
            server.url.clone(),
            "--proxy-user".to_owned(),
            "proxy:secret".to_owned(),
            "http://origin.example/path?q=1".to_owned(),
        ]);
        assert_eq!(
            code,
            0,
            "the proxy request should succeed: {}",
            String::from_utf8_lossy(&stderr)
        );
        assert_eq!(stdout, b"proxy");
        let requests = server.finish();
        let request = String::from_utf8_lossy(&requests[0]);
        assert!(request.starts_with("GET http://origin.example/path?q=1 HTTP/1.1\r\n"));
        assert!(request.contains("Proxy-Authorization: Basic cHJveHk6c2VjcmV0\r\n"));
    }

    #[test]
    fn nonpersistent_fixture_advertises_connection_close() {
        let server = LocalServer::start(vec![
            b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nX-Fixture: keep\r\n\r\none\r\n\x00".to_vec(),
        ]);
        let mut socket = TcpStream::connect(
            server
                .url
                .strip_prefix("http://")
                .expect("fixture uses plaintext HTTP"),
        )
        .expect("fixture should accept a connection");
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("fixture read should be bounded");
        socket
            .write_all(b"GET / HTTP/1.1\r\nHost: fixture.test\r\n\r\n")
            .expect("fixture request should write");
        let mut response = Vec::new();
        let read_result = socket.read_to_end(&mut response);
        let requests = server.finish();
        read_result.expect("fixture should close after one complete response");
        assert_eq!(requests.len(), 1);
        assert_eq!(
            response,
            b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nX-Fixture: keep\r\nConnection: close\r\n\r\none\r\n\x00"
        );
    }

    #[test]
    fn cookies_flow_between_sequential_live_requests() {
        let server = LocalServer::start(vec![
            b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nSet-Cookie: sid=abc; Path=/\r\n\r\none"
                .to_vec(),
            b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\ntwo".to_vec(),
        ]);
        let (code, stdout, stderr) = run(&[
            "-q".to_owned(),
            "-sS".to_owned(),
            format!("{}/one", server.url),
            format!("{}/two", server.url),
        ]);
        assert_eq!(
            code,
            0,
            "both cookie requests should succeed: {}",
            String::from_utf8_lossy(&stderr)
        );
        assert_eq!(stdout, b"onetwo");
        let requests = server.finish();
        assert!(String::from_utf8_lossy(&requests[1]).contains("Cookie: sid=abc\r\n"));
    }

    #[test]
    fn server_cookies_do_not_cross_hosts_on_redirects_or_sequential_transfers() {
        for redirect in [false, true] {
            let destination = LocalServer::start(vec![
                b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ndone".to_vec(),
            ]);
            let destination_url = destination.url.replace("127.0.0.1", "other.test");
            let response = if redirect {
                format!(
                    "HTTP/1.1 302 Found\r\nContent-Length: 0\r\nSet-Cookie: sid=secret; Path=/\r\nLocation: {destination_url}\r\n\r\n"
                )
            } else {
                "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nSet-Cookie: sid=secret; Path=/\r\n\r\n"
                    .to_owned()
            };
            let source = LocalServer::start(vec![response.into_bytes()]);
            let source_url = source.url.replace("127.0.0.1", "source.test");
            let source_port = url::Url::parse(&source_url).unwrap().port().unwrap();
            let destination_port = url::Url::parse(&destination_url).unwrap().port().unwrap();
            let mut arguments = vec![
                "-q".to_owned(),
                "-sS".to_owned(),
                "-L".to_owned(),
                "--noproxy".to_owned(),
                "*".to_owned(),
                "--resolve".to_owned(),
                format!("source.test:{source_port}:127.0.0.1"),
                "--resolve".to_owned(),
                format!("other.test:{destination_port}:127.0.0.1"),
                source_url,
            ];
            if !redirect {
                arguments.push(destination_url);
            }
            let (code, stdout, stderr) = run(&arguments);
            assert_eq!(code, 0, "{}", String::from_utf8_lossy(&stderr));
            assert_eq!(stdout, b"done");
            source.finish();
            let requests = destination.finish();
            assert!(!String::from_utf8_lossy(&requests[0]).contains("Cookie:"));
        }
    }

    #[test]
    fn reuses_an_http11_connection_for_sequential_urls() {
        let server = LocalServer::start_persistent(vec![
            b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\none".to_vec(),
            b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nConnection: close\r\n\r\ntwo".to_vec(),
        ]);
        let (code, stdout, stderr) = run(&[
            "-q".to_owned(),
            "-sS".to_owned(),
            "-m".to_owned(),
            "1".to_owned(),
            format!("{}/one", server.url),
            format!("{}/two", server.url),
        ]);
        assert_eq!(
            code,
            0,
            "both requests should reuse the connection: {}",
            String::from_utf8_lossy(&stderr)
        );
        assert_eq!(stdout, b"onetwo");
        assert_eq!(server.finish().len(), 2);
    }

    #[test]
    fn compressed_response_is_decoded() {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder
            .write_all(b"compressed response")
            .expect("gzip should encode");
        let body = encoder.finish().expect("gzip should finish");
        let mut response = format!(
            "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\n\r\n",
            body.len()
        )
        .into_bytes();
        response.extend_from_slice(&body);
        let server = LocalServer::start(vec![response]);
        let (code, stdout, stderr) = run(&[
            "-q".to_owned(),
            "-sS".to_owned(),
            "--compressed".to_owned(),
            server.url.clone(),
        ]);
        assert_eq!(
            code,
            0,
            "the compressed request should succeed: {}",
            String::from_utf8_lossy(&stderr)
        );
        assert_eq!(stdout, b"compressed response");
        server.finish();
    }

    #[test]
    fn http2_flag_uses_roxygen_http2_against_a_live_roxygen_server() {
        let server = ScopedHttpServer::start(
            ServerConfig {
                plaintext_protocol: CodecProtocol::Http2,
                ..ServerConfig::default()
            },
            handler_factory(),
            None,
        )
        .expect("Roxygen HTTP/2 server should start");
        let address = server.address().expect("server should expose an address");
        let (code, stdout, stderr) = run(&[
            "-q".to_owned(),
            "-sS".to_owned(),
            "-i".to_owned(),
            "--http2".to_owned(),
            format!("http://{address}/"),
        ]);
        assert_eq!(
            code,
            0,
            "the HTTP/2 request should succeed: {}",
            String::from_utf8_lossy(&stderr)
        );
        assert!(
            stdout.starts_with(b"HTTP/2 200 \r\n"),
            "included headers should identify the negotiated protocol"
        );
        assert!(stdout.ends_with(b"roxygen-h2"));
    }

    #[test]
    fn insecure_tls13_http2_works_against_a_live_roxygen_server() {
        let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("test certificate should be generated");
        let tls = TlsServerOptions::new(
            vec![generated.cert.der().to_vec().into_boxed_slice()],
            generated
                .signing_key
                .serialized_der()
                .to_vec()
                .into_boxed_slice(),
            vec![b"h2".to_vec().into_boxed_slice()],
        )
        .expect("test TLS settings should be valid");
        let server = ScopedHttpServer::start(
            ServerConfig {
                tls_credentials: Some(Arc::new(StaticTlsCredentials::new(tls))),
                ..ServerConfig::default()
            },
            handler_factory(),
            None,
        )
        .expect("Roxygen TLS server should start");
        let address = server.address().expect("server should expose an address");
        let (code, stdout, stderr) = run(&[
            "-q".to_owned(),
            "-sS".to_owned(),
            "-k".to_owned(),
            "--http2".to_owned(),
            "--tlsv1.3".to_owned(),
            format!("https://localhost:{}/", address.port()),
        ]);
        assert_eq!(
            code,
            0,
            "the TLS HTTP/2 request should succeed: {}",
            String::from_utf8_lossy(&stderr)
        );
        assert_eq!(stdout, b"roxygen-h2");
    }

    #[test]
    fn cacert_verifies_a_live_roxygen_tls_server() {
        let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("test certificate should be generated");
        let certificate_pem = generated.cert.pem();
        let tls = TlsServerOptions::new(
            vec![generated.cert.der().to_vec().into_boxed_slice()],
            generated
                .signing_key
                .serialized_der()
                .to_vec()
                .into_boxed_slice(),
            vec![b"http/1.1".to_vec().into_boxed_slice()],
        )
        .expect("test TLS settings should be valid");
        let server = ScopedHttpServer::start(
            ServerConfig {
                tls_credentials: Some(Arc::new(StaticTlsCredentials::new(tls))),
                ..ServerConfig::default()
            },
            handler_factory(),
            None,
        )
        .expect("Roxygen TLS server should start");
        let address = server.address().expect("server should expose an address");
        let directory = test_directory("cacert");
        fs::create_dir(&directory).expect("test directory should be created");
        let ca_path = directory.join("ca.pem");
        fs::write(&ca_path, certificate_pem).expect("test CA should be written");
        let (code, stdout, stderr) = run(&[
            "-q".to_owned(),
            "-sS".to_owned(),
            "--cacert".to_owned(),
            ca_path.to_string_lossy().into_owned(),
            "--http1.1".to_owned(),
            format!("https://localhost:{}/", address.port()),
        ]);
        assert_eq!(
            code,
            0,
            "the CA-verified TLS request should succeed: {}",
            String::from_utf8_lossy(&stderr)
        );
        assert_eq!(stdout, b"roxygen-h2");
        fs::remove_dir_all(directory).expect("test directory should be removable");
    }

    #[test]
    fn retries_a_transient_http_response() {
        let server = LocalServer::start(vec![
            b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 5\r\n\r\nretry".to_vec(),
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK".to_vec(),
        ]);
        let (code, stdout, stderr) = run(&[
            "-q".to_owned(),
            "-sS".to_owned(),
            "--retry".to_owned(),
            "1".to_owned(),
            "--retry-delay".to_owned(),
            "0.001".to_owned(),
            server.url.clone(),
        ]);
        assert_eq!(
            code,
            0,
            "the retried request should succeed: {}",
            String::from_utf8_lossy(&stderr)
        );
        assert_eq!(stdout, b"OK", "only the final response should be emitted");
        assert_eq!(server.finish().len(), 2, "one retry should be attempted");
    }

    #[test]
    fn max_time_uses_curl_timeout_exit_code() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("loopback should bind");
        let address = listener.local_addr().expect("listener has an address");
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("client should connect");
            let mut request = [0_u8; 4096];
            let _ = socket.read(&mut request).expect("request should read");
            thread::sleep(Duration::from_millis(100));
            let _ = socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK");
        });
        let (code, stdout, stderr) = run(&[
            "-q".to_owned(),
            "-sS".to_owned(),
            "--max-time".to_owned(),
            "0.01".to_owned(),
            format!("http://{address}/"),
        ]);
        assert_eq!(code, 28, "a total timeout should use curl exit code 28");
        assert!(stdout.is_empty());
        assert!(String::from_utf8_lossy(&stderr).contains("(28)"));
        server.join().expect("server should not panic");
    }

    #[test]
    fn custom_method_auth_and_header_removal_reach_the_server() {
        let server = LocalServer::start(vec![
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".to_vec(),
        ]);
        let (code, _, stderr) = run(&[
            "-q".to_owned(),
            "-sS".to_owned(),
            "-X".to_owned(),
            "PURGE".to_owned(),
            "-u".to_owned(),
            "user:secret".to_owned(),
            "-H".to_owned(),
            "Accept:".to_owned(),
            "-A".to_owned(),
            "test-agent".to_owned(),
            "-e".to_owned(),
            "http://referer.test/".to_owned(),
            server.url.clone(),
        ]);
        assert_eq!(
            code,
            0,
            "the custom request should succeed: {}",
            String::from_utf8_lossy(&stderr)
        );
        let requests = server.finish();
        let request = String::from_utf8_lossy(&requests[0]);
        assert!(request.starts_with("PURGE / HTTP/1.1\r\n"));
        assert!(request.contains("Authorization: Basic dXNlcjpzZWNyZXQ=\r\n"));
        assert!(request.contains("User-Agent: test-agent\r\n"));
        assert!(request.contains("Referer: http://referer.test/\r\n"));
        assert!(!request.contains("Accept: */*\r\n"));
    }

    #[test]
    fn output_diagnostics_and_cookie_jar_are_written() {
        let directory = test_directory("outputs");
        let body_path = directory.join("nested/body.txt");
        let headers_path = directory.join("headers.txt");
        let trace_path = directory.join("trace.txt");
        let cookie_path = directory.join("cookies.txt");
        fs::create_dir(&directory).expect("test directory should be created");
        let server = LocalServer::start(vec![
            b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nSet-Cookie: saved=yes; Path=/\r\n\r\nbody"
                .to_vec(),
        ]);
        let (code, stdout, stderr) = run(&[
            "-q".to_owned(),
            "-sS".to_owned(),
            "--create-dirs".to_owned(),
            "-o".to_owned(),
            body_path.to_string_lossy().into_owned(),
            "-D".to_owned(),
            headers_path.to_string_lossy().into_owned(),
            "--trace-ascii".to_owned(),
            trace_path.to_string_lossy().into_owned(),
            "-c".to_owned(),
            cookie_path.to_string_lossy().into_owned(),
            "-w".to_owned(),
            "%{http_code}:%{size_download}".to_owned(),
            server.url.clone(),
        ]);
        assert_eq!(
            code,
            0,
            "the file-output request should succeed: {}",
            String::from_utf8_lossy(&stderr)
        );
        assert_eq!(stdout, b"200:4");
        assert_eq!(
            fs::read(&body_path).expect("body file should exist"),
            b"body"
        );
        assert!(
            fs::read_to_string(&headers_path)
                .expect("header file should exist")
                .starts_with("HTTP/1.1 200 OK\r\n")
        );
        assert!(
            fs::read_to_string(&trace_path)
                .expect("trace file should exist")
                .contains("=> Send header")
        );
        assert!(
            fs::read_to_string(&cookie_path)
                .expect("cookie jar should exist")
                .contains("saved\tyes")
        );
        server.finish();
        fs::remove_dir_all(&directory).expect("test directory should be removable");
    }

    #[test]
    fn automatic_continue_sends_range_and_appends_at_the_file_offset() {
        for initial in [b"0123".as_slice(), b""] {
            let directory = test_directory("continue");
            fs::create_dir(&directory).expect("test directory should be created");
            let body_path = directory.join("body.txt");
            fs::write(&body_path, initial).expect("partial output should be written");
            let response = if initial.is_empty() {
                b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n0123456789".to_vec()
            } else {
                b"HTTP/1.1 206 Partial Content\r\nContent-Length: 6\r\nContent-Range: bytes 4-9/10\r\n\r\n456789".to_vec()
            };
            let server = LocalServer::start(vec![response]);
            let (code, stdout, stderr) = run(&[
                "-q".to_owned(),
                "-sS".to_owned(),
                "-C".to_owned(),
                "-".to_owned(),
                "-o".to_owned(),
                body_path.to_string_lossy().into_owned(),
                server.url.clone(),
            ]);
            assert_eq!(
                code,
                0,
                "the resumed request should succeed: {}",
                String::from_utf8_lossy(&stderr)
            );
            assert!(stdout.is_empty());
            assert_eq!(
                fs::read(&body_path).expect("resumed output should exist"),
                b"0123456789"
            );
            let requests = server.finish();
            let request = String::from_utf8_lossy(&requests[0]);
            if initial.is_empty() {
                assert!(
                    !request.contains("Range:"),
                    "an empty file needs a full response"
                );
            } else {
                assert!(request.contains("Range: bytes=4-\r\n"));
            }
            fs::remove_dir_all(directory).expect("test directory should be removable");
        }
    }

    #[test]
    fn parallel_mode_completes_multiple_live_transfers() {
        let server = LocalServer::start(vec![
            b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nx".to_vec(),
            b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nx".to_vec(),
        ]);
        let (code, stdout, stderr) = run(&[
            "-q".to_owned(),
            "-sS".to_owned(),
            "-Z".to_owned(),
            format!("{}/one", server.url),
            format!("{}/two", server.url),
        ]);
        assert_eq!(
            code,
            0,
            "parallel transfers should succeed: {}",
            String::from_utf8_lossy(&stderr)
        );
        assert_eq!(stdout, b"xx");
        assert_eq!(server.finish().len(), 2);
    }

    #[test]
    fn resolution_and_connection_failures_use_curl_exit_codes() {
        // Bound with no listener while the test runs, so no other test's listener can take
        // the port and accept the connection.
        let reserved = tokio::net::TcpSocket::new_v4().expect("socket should open");
        reserved
            .bind("127.0.0.1:0".parse().expect("address should parse"))
            .expect("loopback should bind");
        let unused = reserved.local_addr().expect("socket has an address");
        let (connect_code, _, _) = run(&[
            "-q".to_owned(),
            "-sS".to_owned(),
            "--connect-timeout".to_owned(),
            "5".to_owned(),
            format!("http://{unused}/"),
        ]);
        assert_eq!(connect_code, 7, "connection refusal should use curl code 7");

        let (resolve_code, _, _) = run(&[
            "-q".to_owned(),
            "-sS".to_owned(),
            "--connect-timeout".to_owned(),
            "0.5".to_owned(),
            "http://definitely-not-a-host.invalid/".to_owned(),
        ]);
        assert_eq!(resolve_code, 6, "DNS failure should use curl code 6");
    }
}
