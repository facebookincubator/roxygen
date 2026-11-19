/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::VecDeque;
use std::env;
use std::ffi::OsString;
use std::fs;
use std::io::Read;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use url::Url as ParsedUrl;

use crate::CliError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum OutputTarget {
    Stdout,
    File(PathBuf),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum IpVersion {
    #[default]
    Any,
    V4,
    V6,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum HttpVersion {
    #[default]
    Any,
    Http11,
    Http2,
    Http3,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum MinimumTlsVersion {
    #[default]
    Default,
    Tls12,
    Tls13,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HeaderArgument {
    pub name: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResolveEntry {
    pub host: String,
    pub port: u16,
    pub addresses: Vec<IpAddr>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum OutputArgument {
    File(PathBuf),
    RemoteName,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DataKind {
    Data,
    Raw,
    Binary,
    UrlEncode,
    Json,
}

#[derive(Debug, thiserror::Error)]
enum ConfigSyntaxError {
    #[error("unterminated quoted value")]
    UnterminatedQuote,
    #[error("quoted value ends with an escape")]
    TrailingEscape,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DataArgument {
    kind: DataKind,
    value: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RequestBody {
    pub bytes: Vec<u8>,
    pub content_type: Option<Vec<u8>>,
    pub implied_method: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TransferSpec {
    pub url: String,
    pub output: OutputTarget,
    pub resume_offset: Option<u64>,
}

#[derive(Clone, Debug)]
pub(crate) struct Options {
    pub request: Option<String>,
    pub headers: Vec<HeaderArgument>,
    pub body: Option<RequestBody>,
    pub location: bool,
    pub include: bool,
    pub head: bool,
    pub silent: bool,
    pub show_error: bool,
    pub verbose: bool,
    pub fail: bool,
    pub user: Option<String>,
    pub insecure: bool,
    pub user_agent: Option<String>,
    pub referer: Option<String>,
    pub get: bool,
    pub max_time: Option<Duration>,
    pub connect_timeout: Duration,
    pub retry: u32,
    pub retry_delay: Duration,
    pub retry_max_time: Option<Duration>,
    pub cookies: Vec<String>,
    pub cookie_jar: Option<PathBuf>,
    pub write_out: Option<String>,
    pub dump_header: Option<PathBuf>,
    pub trace: Option<PathBuf>,
    pub trace_ascii: Option<PathBuf>,
    pub progress_bar: bool,
    pub create_dirs: bool,
    pub proxy: Option<String>,
    pub proxy_user: Option<String>,
    pub no_proxy: Option<String>,
    pub compressed: bool,
    pub resolve: Vec<ResolveEntry>,
    pub dns_servers: Vec<SocketAddr>,
    pub ip_version: IpVersion,
    pub cacert: Option<PathBuf>,
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
    pub http_version: HttpVersion,
    pub minimum_tls_version: MinimumTlsVersion,
    pub range: Option<String>,
    pub parallel: bool,
    pub limit_rate: Option<u64>,
    pub netrc: bool,
    pub keepalive: bool,
    pub transfers: Vec<TransferSpec>,
    pub help: bool,
    pub version: bool,
}

#[derive(Default)]
struct RawOptions {
    request: Option<String>,
    headers: Vec<HeaderArgument>,
    data: Vec<DataArgument>,
    forms: Vec<String>,
    upload_file: Option<String>,
    outputs: Vec<OutputArgument>,
    urls: Vec<String>,
    location: bool,
    include: bool,
    head: bool,
    silent: bool,
    show_error: bool,
    verbose: bool,
    fail: bool,
    user: Option<String>,
    insecure: bool,
    user_agent: Option<String>,
    referer: Option<String>,
    get: bool,
    max_time: Option<Duration>,
    connect_timeout: Option<Duration>,
    retry: u32,
    retry_delay: Option<Duration>,
    retry_max_time: Option<Duration>,
    cookies: Vec<String>,
    cookie_jar: Option<PathBuf>,
    write_out: Option<String>,
    dump_header: Option<PathBuf>,
    trace: Option<PathBuf>,
    trace_ascii: Option<PathBuf>,
    progress_bar: bool,
    create_dirs: bool,
    proxy: Option<String>,
    proxy_user: Option<String>,
    no_proxy: Option<String>,
    compressed: bool,
    resolve: Vec<ResolveEntry>,
    dns_servers: Vec<SocketAddr>,
    ip_version: IpVersion,
    cacert: Option<PathBuf>,
    cert: Option<PathBuf>,
    key: Option<PathBuf>,
    http_version: HttpVersion,
    minimum_tls_version: MinimumTlsVersion,
    range: Option<String>,
    continue_at: Option<String>,
    parallel: bool,
    limit_rate: Option<u64>,
    disable: bool,
    netrc: bool,
    keepalive: bool,
    help: bool,
    version: bool,
    config_depth: usize,
}

pub(crate) fn parse(
    arguments: impl IntoIterator<Item = OsString>,
    stdin: &mut dyn Read,
) -> Result<Options, CliError> {
    let arguments = arguments
        .into_iter()
        .map(|argument| {
            argument
                .into_string()
                .map_err(|_| CliError::Usage("arguments must be valid UTF-8".to_owned()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let disable = command_line_disables_config(&arguments);
    let mut raw = RawOptions {
        disable,
        keepalive: true,
        ..RawOptions::default()
    };
    if !disable && let Some(path) = default_config_path() {
        raw.parse_config_path(&path, false)?;
    }
    raw.parse_tokens(arguments)?;
    raw.finish(stdin)
}

fn command_line_disables_config(arguments: &[String]) -> bool {
    arguments
        .first()
        .is_some_and(|argument| argument == "--disable" || argument.starts_with("-q"))
}

fn default_config_path() -> Option<PathBuf> {
    if let Some(home) = env::var_os("CURL_HOME") {
        return Some(PathBuf::from(home).join(".curlrc"));
    }
    if let Some(home) = env::var_os("XDG_CONFIG_HOME") {
        return Some(PathBuf::from(home).join("curlrc"));
    }
    env::var_os("HOME").map(|home| PathBuf::from(home).join(".curlrc"))
}

impl RawOptions {
    fn parse_tokens(&mut self, arguments: Vec<String>) -> Result<(), CliError> {
        let mut arguments = VecDeque::from(arguments);
        let mut options_enabled = true;
        while let Some(argument) = arguments.pop_front() {
            if options_enabled && argument == "--" {
                options_enabled = false;
                continue;
            }
            if options_enabled && argument.starts_with("--") {
                self.parse_long(argument, &mut arguments)?;
            } else if options_enabled && argument.starts_with('-') && argument.len() > 1 {
                self.parse_short(&argument[1..], &mut arguments)?;
            } else {
                self.urls.push(argument);
            }
        }
        Ok(())
    }

    fn parse_long(
        &mut self,
        argument: String,
        remaining: &mut VecDeque<String>,
    ) -> Result<(), CliError> {
        let (name, inline_value) = match argument.split_once('=') {
            Some((name, value)) => (name, Some(value.to_owned())),
            None => (argument.as_str(), None),
        };
        let mut value = || {
            inline_value
                .clone()
                .or_else(|| remaining.pop_front())
                .ok_or_else(|| CliError::Usage(format!("option {name} requires an argument")))
        };
        match name {
            "--request" => self.request = Some(value()?),
            "--header" => self.headers.push(parse_header(&value()?)?),
            "--data" => self.push_data(DataKind::Data, value()?),
            "--data-raw" => self.push_data(DataKind::Raw, value()?),
            "--data-binary" => self.push_data(DataKind::Binary, value()?),
            "--data-urlencode" => self.push_data(DataKind::UrlEncode, value()?),
            "--json" => self.push_data(DataKind::Json, value()?),
            "--output" => self.outputs.push(OutputArgument::File(value()?.into())),
            "--remote-name" => self.outputs.push(OutputArgument::RemoteName),
            "--no-remote-name" => {
                if self.outputs.last() == Some(&OutputArgument::RemoteName) {
                    self.outputs.pop();
                }
            }
            "--location" => self.location = true,
            "--no-location" => self.location = false,
            "--include" => self.include = true,
            "--no-include" => self.include = false,
            "--head" => self.head = true,
            "--no-head" => self.head = false,
            "--silent" => self.silent = true,
            "--no-silent" => self.silent = false,
            "--show-error" => self.show_error = true,
            "--no-show-error" => self.show_error = false,
            "--verbose" => self.verbose = true,
            "--no-verbose" => self.verbose = false,
            "--fail" => self.fail = true,
            "--no-fail" => self.fail = false,
            "--user" => self.user = Some(value()?),
            "--insecure" => self.insecure = true,
            "--no-insecure" => self.insecure = false,
            "--user-agent" => self.user_agent = Some(value()?),
            "--referer" => self.referer = Some(value()?),
            "--form" => self.forms.push(value()?),
            "--upload-file" => self.upload_file = Some(value()?),
            "--get" => self.get = true,
            "--no-get" => self.get = false,
            "--max-time" => self.max_time = nonzero_duration(parse_duration(name, &value()?)?),
            "--connect-timeout" => {
                self.connect_timeout = nonzero_duration(parse_duration(name, &value()?)?)
            }
            "--retry" => self.retry = parse_number(name, &value()?)?,
            "--retry-delay" => self.retry_delay = Some(parse_duration(name, &value()?)?),
            "--retry-max-time" => {
                self.retry_max_time = nonzero_duration(parse_duration(name, &value()?)?)
            }
            "--cookie" => self.cookies.push(value()?),
            "--cookie-jar" => self.cookie_jar = Some(value()?.into()),
            "--write-out" => self.write_out = Some(value()?),
            "--dump-header" => self.dump_header = Some(value()?.into()),
            "--trace" => self.trace = Some(value()?.into()),
            "--trace-ascii" => self.trace_ascii = Some(value()?.into()),
            "--progress-bar" => self.progress_bar = true,
            "--no-progress-bar" => self.progress_bar = false,
            "--create-dirs" => self.create_dirs = true,
            "--no-create-dirs" => self.create_dirs = false,
            "--proxy" => self.proxy = Some(value()?),
            "--proxy-user" => self.proxy_user = Some(value()?),
            "--noproxy" => self.no_proxy = Some(value()?),
            "--compressed" => self.compressed = true,
            "--no-compressed" => self.compressed = false,
            "--resolve" => self.resolve.push(parse_resolve(&value()?)?),
            "--dns-servers" => self.dns_servers = parse_dns_servers(&value()?)?,
            "--ipv4" => self.ip_version = IpVersion::V4,
            "--no-ipv4" => self.ip_version = IpVersion::Any,
            "--ipv6" => self.ip_version = IpVersion::V6,
            "--no-ipv6" => self.ip_version = IpVersion::Any,
            "--cacert" => self.cacert = Some(value()?.into()),
            "--cert" => self.cert = Some(value()?.into()),
            "--key" => self.key = Some(value()?.into()),
            "--http1.1" => self.http_version = HttpVersion::Http11,
            "--no-http1.1" => self.http_version = HttpVersion::Any,
            "--http2" => self.http_version = HttpVersion::Http2,
            "--no-http2" => self.http_version = HttpVersion::Any,
            "--http3" => self.http_version = HttpVersion::Http3,
            "--no-http3" => self.http_version = HttpVersion::Any,
            "--tlsv1.2" => self.minimum_tls_version = MinimumTlsVersion::Tls12,
            "--no-tlsv1.2" => self.minimum_tls_version = MinimumTlsVersion::Default,
            "--tlsv1.3" => self.minimum_tls_version = MinimumTlsVersion::Tls13,
            "--no-tlsv1.3" => self.minimum_tls_version = MinimumTlsVersion::Default,
            "--range" => self.range = Some(value()?),
            "--continue-at" => self.continue_at = Some(value()?),
            "--parallel" => self.parallel = true,
            "--no-parallel" => self.parallel = false,
            "--limit-rate" => self.limit_rate = Some(parse_rate(&value()?)?),
            "--config" => self.parse_config_path(Path::new(&value()?), true)?,
            "--disable" => self.disable = true,
            "--no-disable" => self.disable = false,
            "--netrc" => self.netrc = true,
            "--no-netrc" => self.netrc = false,
            "--keepalive" => self.keepalive = true,
            "--no-keepalive" => self.keepalive = false,
            "--version" => self.version = true,
            "--help" => self.help = true,
            "--url" => self.urls.push(value()?),
            _ => return Err(CliError::Usage(format!("unknown option: {name}"))),
        }
        Ok(())
    }

    fn parse_short(
        &mut self,
        argument: &str,
        remaining: &mut VecDeque<String>,
    ) -> Result<(), CliError> {
        let mut chars = argument.char_indices().peekable();
        while let Some((_, option)) = chars.next() {
            let needs_value = matches!(
                option,
                'X' | 'H'
                    | 'd'
                    | 'o'
                    | 'u'
                    | 'A'
                    | 'e'
                    | 'F'
                    | 'T'
                    | 'm'
                    | 'b'
                    | 'c'
                    | 'w'
                    | 'D'
                    | 'x'
                    | 'r'
                    | 'C'
                    | 'K'
            );
            let option_value = if needs_value {
                let rest_index = chars.peek().map_or(argument.len(), |(index, _)| *index);
                let rest = &argument[rest_index..];
                let result = if rest.is_empty() {
                    remaining.pop_front()
                } else {
                    Some(rest.to_owned())
                }
                .ok_or_else(|| CliError::Usage(format!("option -{option} requires an argument")))?;
                while chars.next().is_some() {}
                Some(result)
            } else {
                None
            };
            match option {
                'X' => self.request = option_value,
                'H' => self.headers.push(parse_header(
                    option_value.as_deref().expect("value was required"),
                )?),
                'd' => self.push_data(DataKind::Data, option_value.expect("value was required")),
                'o' => self.outputs.push(OutputArgument::File(
                    option_value.expect("value was required").into(),
                )),
                'O' => self.outputs.push(OutputArgument::RemoteName),
                'L' => self.location = true,
                'i' => self.include = true,
                'I' => self.head = true,
                's' => self.silent = true,
                'S' => self.show_error = true,
                'v' => self.verbose = true,
                'f' => self.fail = true,
                'u' => self.user = option_value,
                'k' => self.insecure = true,
                'A' => self.user_agent = option_value,
                'e' => self.referer = option_value,
                'F' => self.forms.push(option_value.expect("value was required")),
                'T' => self.upload_file = option_value,
                'G' => self.get = true,
                'm' => {
                    self.max_time = nonzero_duration(parse_duration(
                        "-m",
                        option_value.as_deref().expect("value was required"),
                    )?)
                }
                'b' => self.cookies.push(option_value.expect("value was required")),
                'c' => self.cookie_jar = Some(option_value.expect("value was required").into()),
                'w' => self.write_out = option_value,
                'D' => self.dump_header = Some(option_value.expect("value was required").into()),
                '#' => self.progress_bar = true,
                'x' => self.proxy = option_value,
                '4' => self.ip_version = IpVersion::V4,
                '6' => self.ip_version = IpVersion::V6,
                'r' => self.range = option_value,
                'C' => self.continue_at = option_value,
                'Z' => self.parallel = true,
                'K' => self.parse_config_path(
                    Path::new(option_value.as_deref().expect("value was required")),
                    true,
                )?,
                'q' => self.disable = true,
                'n' => self.netrc = true,
                'V' => self.version = true,
                'h' => self.help = true,
                _ => return Err(CliError::Usage(format!("unknown option: -{option}"))),
            }
        }
        Ok(())
    }

    fn push_data(&mut self, kind: DataKind, value: String) {
        self.data.push(DataArgument { kind, value });
    }

    fn parse_config_path(&mut self, path: &Path, required: bool) -> Result<(), CliError> {
        if self.config_depth >= 20 {
            return Err(CliError::Usage(
                "config files are nested more than 20 levels".to_owned(),
            ));
        }
        let contents = match fs::read_to_string(path) {
            Ok(contents) => contents,
            Err(error) if !required && error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(());
            }
            Err(error) => {
                return Err(CliError::Input {
                    path: path.to_path_buf(),
                    source: error,
                });
            }
        };
        self.config_depth += 1;
        for (index, line) in contents.lines().enumerate() {
            let tokens = config_line_tokens(line).map_err(|message| {
                CliError::Usage(format!("{}:{}: {message}", path.display(), index + 1))
            })?;
            self.parse_tokens(tokens)?;
        }
        self.config_depth -= 1;
        Ok(())
    }

    fn finish(self, stdin: &mut dyn Read) -> Result<Options, CliError> {
        if !self.help && !self.version && self.urls.is_empty() {
            return Err(CliError::Usage("no URL specified".to_owned()));
        }
        if !self.forms.is_empty() && (!self.data.is_empty() || self.upload_file.is_some()) {
            return Err(CliError::Usage(
                "--form cannot be combined with data or --upload-file".to_owned(),
            ));
        }
        if self.upload_file.is_some() && !self.data.is_empty() {
            return Err(CliError::Usage(
                "--upload-file cannot be combined with data options".to_owned(),
            ));
        }
        if self.get && (!self.forms.is_empty() || self.upload_file.is_some()) {
            return Err(CliError::Usage(
                "--get only moves data-option content into the query string".to_owned(),
            ));
        }
        if self
            .data
            .iter()
            .any(|argument| argument.kind == DataKind::Json)
            && (!self.forms.is_empty() || self.head || self.upload_file.is_some())
        {
            return Err(CliError::Usage(
                "--json cannot be combined with --form, --head, or --upload-file".to_owned(),
            ));
        }
        if self.cert.is_some() != self.key.is_some() {
            return Err(CliError::Usage(
                "--cert and --key must be supplied together".to_owned(),
            ));
        }
        if self.http_version == HttpVersion::Http3 {
            return Err(CliError::Unsupported(
                "HTTP/3 is outside Roxygen's HTTP/1.1 and HTTP/2 transport scope".to_owned(),
            ));
        }
        if self
            .request
            .as_deref()
            .is_some_and(|method| method.eq_ignore_ascii_case("CONNECT"))
        {
            return Err(CliError::Unsupported(
                "CONNECT is not supported by Roxygen".to_owned(),
            ));
        }

        let mut stdin_bytes = None;
        let body = prepare_body(
            &self.data,
            &self.forms,
            self.upload_file.as_deref(),
            stdin,
            &mut stdin_bytes,
        )?;
        let write_out = self
            .write_out
            .as_deref()
            .map(|value| read_at_argument(value, stdin, &mut stdin_bytes))
            .transpose()?;
        let transfers = self
            .urls
            .iter()
            .enumerate()
            .map(|(index, url)| {
                let output = match self.outputs.get(index) {
                    Some(OutputArgument::File(path)) if path == Path::new("-") => {
                        OutputTarget::Stdout
                    }
                    Some(OutputArgument::File(path)) => OutputTarget::File(path.clone()),
                    Some(OutputArgument::RemoteName) => {
                        OutputTarget::File(remote_name(url)?.into())
                    }
                    None => OutputTarget::Stdout,
                };
                let resume_offset = match self.continue_at.as_deref() {
                    None => None,
                    Some("-") => match &output {
                        OutputTarget::File(path) => Some(fs::metadata(path).map_or(0, |m| m.len())),
                        OutputTarget::Stdout => {
                            return Err(CliError::Usage("-C - requires an output file".to_owned()));
                        }
                    },
                    Some(offset) => Some(parse_number("--continue-at", offset)?),
                };
                Ok(TransferSpec {
                    url: url.clone(),
                    output,
                    resume_offset,
                })
            })
            .collect::<Result<Vec<_>, CliError>>()?;

        Ok(Options {
            request: self.request,
            headers: self.headers,
            body,
            location: self.location,
            include: self.include,
            head: self.head,
            silent: self.silent,
            show_error: self.show_error,
            verbose: self.verbose,
            fail: self.fail,
            user: self.user,
            insecure: self.insecure,
            user_agent: self.user_agent,
            referer: self.referer,
            get: self.get,
            max_time: self.max_time,
            connect_timeout: self
                .connect_timeout
                .unwrap_or_else(|| Duration::from_secs(300)),
            retry: self.retry,
            retry_delay: self.retry_delay.unwrap_or(Duration::ZERO),
            retry_max_time: self.retry_max_time,
            cookies: self.cookies,
            cookie_jar: self.cookie_jar,
            write_out,
            dump_header: self.dump_header,
            trace: self.trace,
            trace_ascii: self.trace_ascii,
            progress_bar: self.progress_bar,
            create_dirs: self.create_dirs,
            proxy: self.proxy.filter(|proxy| !proxy.is_empty()),
            proxy_user: self.proxy_user,
            no_proxy: self.no_proxy,
            compressed: self.compressed,
            resolve: self.resolve,
            dns_servers: self.dns_servers,
            ip_version: self.ip_version,
            cacert: self.cacert,
            cert: self.cert,
            key: self.key,
            http_version: self.http_version,
            minimum_tls_version: self.minimum_tls_version,
            range: self.range,
            parallel: self.parallel,
            limit_rate: self.limit_rate,
            netrc: self.netrc,
            keepalive: self.keepalive,
            transfers,
            help: self.help,
            version: self.version,
        })
    }
}

fn nonzero_duration(duration: Duration) -> Option<Duration> {
    (duration != Duration::ZERO).then_some(duration)
}

fn parse_duration(option: &str, value: &str) -> Result<Duration, CliError> {
    let seconds = value
        .parse::<f64>()
        .map_err(|_| CliError::Usage(format!("invalid duration for {option}: {value}")))?;
    if !seconds.is_finite() || seconds < 0.0 {
        return Err(CliError::Usage(format!(
            "invalid duration for {option}: {value}"
        )));
    }
    Ok(Duration::from_secs_f64(seconds))
}

fn parse_number<T>(option: &str, value: &str) -> Result<T, CliError>
where
    T: std::str::FromStr,
{
    value
        .parse()
        .map_err(|_| CliError::Usage(format!("invalid number for {option}: {value}")))
}

fn parse_rate(value: &str) -> Result<u64, CliError> {
    let value = value.strip_suffix("/s").unwrap_or(value);
    let (number, multiplier) = match value.as_bytes().last().copied() {
        Some(b'k' | b'K') => (&value[..value.len() - 1], 1024_u64),
        Some(b'm' | b'M') => (&value[..value.len() - 1], 1024_u64.pow(2)),
        Some(b'g' | b'G') => (&value[..value.len() - 1], 1024_u64.pow(3)),
        _ => (value, 1),
    };
    let rate = parse_number::<u64>("--limit-rate", number)?
        .checked_mul(multiplier)
        .ok_or_else(|| CliError::Usage(format!("rate is too large: {value}")))?;
    if rate == 0 {
        return Err(CliError::Usage(
            "--limit-rate must be greater than zero".to_owned(),
        ));
    }
    Ok(rate)
}

fn parse_header(value: &str) -> Result<HeaderArgument, CliError> {
    if let Some(name) = value.strip_suffix(';')
        && !name.contains(':')
    {
        return header_argument(name, Some(Vec::new()));
    }
    let Some((name, value)) = value.split_once(':') else {
        return Err(CliError::Usage(format!(
            "header must contain a colon: {value}"
        )));
    };
    let value = value.trim_start_matches([' ', '\t']);
    header_argument(name, (!value.is_empty()).then(|| value.as_bytes().to_vec()))
}

fn header_argument(name: &str, value: Option<Vec<u8>>) -> Result<HeaderArgument, CliError> {
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && byte != b':')
    {
        return Err(CliError::Usage(format!("invalid header name: {name}")));
    }
    Ok(HeaderArgument {
        name: name.as_bytes().to_vec(),
        value,
    })
}

fn parse_resolve(value: &str) -> Result<ResolveEntry, CliError> {
    let (host, remainder) = split_host_field(value)
        .ok_or_else(|| CliError::Usage(format!("invalid --resolve entry: {value}")))?;
    let (port, addresses) = remainder
        .split_once(':')
        .ok_or_else(|| CliError::Usage(format!("invalid --resolve entry: {value}")))?;
    let port = parse_number("--resolve port", port)?;
    let addresses = addresses
        .split(',')
        .map(|address| {
            address
                .trim_matches(['[', ']'])
                .parse::<IpAddr>()
                .map_err(|_| CliError::Usage(format!("invalid --resolve address: {address}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if addresses.is_empty() {
        return Err(CliError::Usage(format!(
            "--resolve has no addresses: {value}"
        )));
    }
    Ok(ResolveEntry {
        host: host.trim_matches(['[', ']']).to_owned(),
        port,
        addresses,
    })
}

fn split_host_field(value: &str) -> Option<(&str, &str)> {
    if value.starts_with('[') {
        let end = value.find(']')?;
        return value
            .get(end + 1..)?
            .strip_prefix(':')
            .map(|rest| (&value[..=end], rest));
    }
    value.split_once(':')
}

fn parse_dns_servers(value: &str) -> Result<Vec<SocketAddr>, CliError> {
    value
        .split(',')
        .map(|server| {
            if let Ok(address) = server.parse::<SocketAddr>() {
                return Ok(address);
            }
            server
                .trim_matches(['[', ']'])
                .parse::<IpAddr>()
                .map(|address| SocketAddr::new(address, 53))
                .map_err(|_| CliError::Usage(format!("invalid DNS server: {server}")))
        })
        .collect()
}

fn prepare_body(
    data: &[DataArgument],
    forms: &[String],
    upload_file: Option<&str>,
    stdin: &mut dyn Read,
    stdin_bytes: &mut Option<Vec<u8>>,
) -> Result<Option<RequestBody>, CliError> {
    if !forms.is_empty() {
        return prepare_form(forms, stdin, stdin_bytes).map(Some);
    }
    if let Some(path) = upload_file {
        return Ok(Some(RequestBody {
            bytes: read_data_file(path, false, stdin, stdin_bytes)?,
            content_type: None,
            implied_method: "PUT",
        }));
    }
    if data.is_empty() {
        return Ok(None);
    }
    let mut parts = Vec::with_capacity(data.len());
    let mut json = false;
    for argument in data {
        json |= argument.kind == DataKind::Json;
        let bytes = match argument.kind {
            DataKind::Raw => argument.value.as_bytes().to_vec(),
            DataKind::Data => {
                if let Some(path) = argument.value.strip_prefix('@') {
                    read_data_file(path, true, stdin, stdin_bytes)?
                } else {
                    argument.value.as_bytes().to_vec()
                }
            }
            DataKind::Binary | DataKind::Json => {
                if let Some(path) = argument.value.strip_prefix('@') {
                    read_data_file(path, false, stdin, stdin_bytes)?
                } else {
                    argument.value.as_bytes().to_vec()
                }
            }
            DataKind::UrlEncode => prepare_urlencoded(&argument.value, stdin, stdin_bytes)?,
        };
        parts.push(bytes);
    }
    let bytes = parts.join(&b'&');
    Ok(Some(RequestBody {
        bytes,
        content_type: Some(if json {
            b"application/json".to_vec()
        } else {
            b"application/x-www-form-urlencoded".to_vec()
        }),
        implied_method: "POST",
    }))
}

fn prepare_urlencoded(
    argument: &str,
    stdin: &mut dyn Read,
    stdin_bytes: &mut Option<Vec<u8>>,
) -> Result<Vec<u8>, CliError> {
    if let Some(path) = argument.strip_prefix('@') {
        return Ok(percent_encode(&read_data_file(
            path,
            false,
            stdin,
            stdin_bytes,
        )?));
    }
    if let Some((name, content)) = argument.split_once('=') {
        let mut result = name.as_bytes().to_vec();
        if !name.is_empty() {
            result.push(b'=');
        }
        result.extend(percent_encode(content.as_bytes()));
        return Ok(result);
    }
    if let Some((name, path)) = argument.split_once('@') {
        let mut result = name.as_bytes().to_vec();
        result.push(b'=');
        result.extend(percent_encode(&read_data_file(
            path,
            false,
            stdin,
            stdin_bytes,
        )?));
        return Ok(result);
    }
    Ok(percent_encode(argument.as_bytes()))
}

fn percent_encode(bytes: &[u8]) -> Vec<u8> {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    bytes
        .iter()
        .fold(Vec::with_capacity(bytes.len()), |mut encoded, byte| {
            if byte.is_ascii_alphanumeric() || b"-._~".contains(byte) {
                encoded.push(*byte);
            } else {
                encoded.extend_from_slice(&[
                    b'%',
                    HEX[usize::from(*byte >> 4)],
                    HEX[usize::from(*byte & 0x0f)],
                ]);
            }
            encoded
        })
}

struct FormContent {
    value: String,
    content_type: Option<String>,
    filename: Option<String>,
}

fn parse_form_content(content: &str) -> FormContent {
    let mut parts = content.split(';');
    let mut value = parts.next().unwrap_or_default().to_owned();
    let mut content_type = None;
    let mut filename = None;
    for part in parts {
        if let Some(value) = part.strip_prefix("type=") {
            content_type = Some(value.trim_matches('"').to_owned());
        } else if let Some(value) = part.strip_prefix("filename=") {
            filename = Some(value.trim_matches('"').to_owned());
        } else {
            value.push(';');
            value.push_str(part);
        }
    }
    FormContent {
        value,
        content_type,
        filename,
    }
}

fn prepare_form(
    forms: &[String],
    stdin: &mut dyn Read,
    stdin_bytes: &mut Option<Vec<u8>>,
) -> Result<RequestBody, CliError> {
    let mut parts = Vec::with_capacity(forms.len());
    for form in forms {
        let Some((name, content)) = form.split_once('=') else {
            return Err(CliError::Usage(format!(
                "form field must use name=content: {form}"
            )));
        };
        let mut body = Vec::new();
        body.extend_from_slice(b"Content-Disposition: form-data; name=\"");
        body.extend_from_slice(name.replace(['\r', '\n', '"'], "_").as_bytes());
        body.push(b'"');
        let form_content = parse_form_content(content);
        let (contents, default_filename) = match form_content.value.as_bytes().first() {
            Some(b'@') => {
                let path = &form_content.value[1..];
                (
                    read_data_file(path, false, stdin, stdin_bytes)?,
                    Path::new(path)
                        .file_name()
                        .and_then(|name| name.to_str())
                        .map(str::to_owned),
                )
            }
            Some(b'<') => (
                read_data_file(&form_content.value[1..], false, stdin, stdin_bytes)?,
                None,
            ),
            _ => (form_content.value.as_bytes().to_vec(), None),
        };
        let content_type = form_content.content_type.or_else(|| {
            default_filename
                .as_ref()
                .map(|_| "application/octet-stream".to_owned())
        });
        let filename = form_content.filename.or(default_filename);
        if let Some(filename) = filename {
            body.extend_from_slice(b"; filename=\"");
            body.extend_from_slice(filename.replace(['\r', '\n', '"'], "_").as_bytes());
            body.push(b'"');
        }
        if let Some(content_type) = content_type {
            body.extend_from_slice(b"\r\nContent-Type: ");
            body.extend_from_slice(content_type.replace(['\r', '\n'], "_").as_bytes());
        }
        body.extend_from_slice(b"\r\n\r\n");
        body.extend_from_slice(&contents);
        body.extend_from_slice(b"\r\n");
        parts.push(body);
    }
    let prefix = format!("------------------------roxygen{:x}", std::process::id());
    let boundary = (0_u64..)
        .map(|attempt| format!("{prefix}-{attempt:x}"))
        .find(|candidate| {
            parts.iter().all(|part| {
                !part
                    .windows(candidate.len())
                    .any(|window| window == candidate.as_bytes())
            })
        })
        .expect("finite multipart contents cannot contain every boundary candidate");
    let mut body = Vec::new();
    for part in parts {
        body.extend_from_slice(b"--");
        body.extend_from_slice(boundary.as_bytes());
        body.extend_from_slice(b"\r\n");
        body.extend_from_slice(&part);
    }
    body.extend_from_slice(b"--");
    body.extend_from_slice(boundary.as_bytes());
    body.extend_from_slice(b"--\r\n");
    Ok(RequestBody {
        bytes: body,
        content_type: Some(format!("multipart/form-data; boundary={boundary}").into_bytes()),
        implied_method: "POST",
    })
}

fn read_data_file(
    path: &str,
    strip_newlines: bool,
    stdin: &mut dyn Read,
    stdin_bytes: &mut Option<Vec<u8>>,
) -> Result<Vec<u8>, CliError> {
    let mut bytes = if path == "-" {
        if stdin_bytes.is_none() {
            let mut contents = Vec::new();
            stdin.read_to_end(&mut contents).map_err(CliError::Stdin)?;
            *stdin_bytes = Some(contents);
        }
        stdin_bytes
            .as_ref()
            .expect("stdin contents were initialized")
            .clone()
    } else {
        fs::read(path).map_err(|source| CliError::Input {
            path: path.into(),
            source,
        })?
    };
    if strip_newlines {
        bytes.retain(|byte| !matches!(byte, b'\r' | b'\n'));
    }
    Ok(bytes)
}

fn remote_name(url: &str) -> Result<String, CliError> {
    let parsed = ParsedUrl::parse(url)
        .map_err(|error| CliError::Usage(format!("invalid URL {url:?}: {error}")))?;
    parsed
        .path_segments()
        .and_then(|mut segments| segments.next_back())
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| CliError::Usage(format!("URL has no remote file name: {url}")))
}

fn read_at_argument(
    value: &str,
    stdin: &mut dyn Read,
    stdin_bytes: &mut Option<Vec<u8>>,
) -> Result<String, CliError> {
    if value == "@-" {
        return String::from_utf8(read_data_file("-", false, stdin, stdin_bytes)?)
            .map_err(|_| CliError::Usage("--write-out input must be valid UTF-8".to_owned()));
    }
    let Some(path) = value.strip_prefix('@') else {
        return Ok(value.to_owned());
    };
    fs::read_to_string(path).map_err(|source| CliError::Input {
        path: path.into(),
        source,
    })
}

fn config_line_tokens(line: &str) -> Result<Vec<String>, ConfigSyntaxError> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return Ok(Vec::new());
    }
    let split = line
        .char_indices()
        .find(|(_, character)| character.is_whitespace() || matches!(character, '=' | ':'));
    let (name, value) = match split {
        Some((index, _)) => {
            let rest = line[index..]
                .trim_start_matches(|character: char| {
                    character.is_whitespace() || matches!(character, '=' | ':')
                })
                .trim();
            (line[..index].trim(), (!rest.is_empty()).then_some(rest))
        }
        None => (line, None),
    };
    let option = if name.starts_with('-') {
        name.to_owned()
    } else {
        format!("--{name}")
    };
    let mut tokens = vec![option];
    if let Some(value) = value {
        tokens.push(unquote(value)?);
    }
    Ok(tokens)
}

fn unquote(value: &str) -> Result<String, ConfigSyntaxError> {
    if !value.starts_with('"') {
        return Ok(value.to_owned());
    }
    if !value.ends_with('"') || value.len() == 1 {
        return Err(ConfigSyntaxError::UnterminatedQuote);
    }
    let mut escaped = false;
    let mut result = String::new();
    for character in value[1..value.len() - 1].chars() {
        if escaped {
            result.push(match character {
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                other => other,
            });
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else {
            result.push(character);
        }
    }
    if escaped {
        return Err(ConfigSyntaxError::TrailingEscape);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::fs;
    use std::io::Cursor;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::*;

    fn options(arguments: &[&str]) -> Options {
        let arguments =
            std::iter::once(OsString::from("-q")).chain(arguments.iter().map(OsString::from));
        parse(arguments, &mut Cursor::new(Vec::new())).expect("arguments should parse")
    }

    #[test]
    fn config_is_disabled_only_by_the_first_option() {
        for arguments in [vec!["-q"], vec!["-qs"], vec!["--disable"]] {
            let arguments = arguments.into_iter().map(str::to_owned).collect::<Vec<_>>();
            assert!(command_line_disables_config(&arguments));
        }
        for arguments in [
            vec!["-o", "-q"],
            vec!["-d", "-q"],
            vec!["--output", "--disable"],
            vec!["-s", "-q"],
            vec!["-sq"],
            vec!["--", "-q"],
        ] {
            let arguments = arguments.into_iter().map(str::to_owned).collect::<Vec<_>>();
            assert!(!command_line_disables_config(&arguments), "{arguments:?}");
        }
    }

    #[test]
    fn parses_clustered_short_options_and_positional_outputs() {
        let options = options(&[
            "-sSL",
            "-o",
            "first",
            "-O",
            "http://example.test/a",
            "http://example.test/b",
        ]);
        assert!(options.silent, "-s should enable silent mode");
        assert!(options.show_error, "-S should retain error output");
        assert!(options.location, "-L should enable redirects");
        assert_eq!(
            options.transfers[0].output,
            OutputTarget::File(PathBuf::from("first")),
            "the first output option should pair with the first URL"
        );
        assert_eq!(
            options.transfers[1].output,
            OutputTarget::File(PathBuf::from("b")),
            "-O should derive the second URL's basename"
        );
    }

    #[test]
    fn data_variants_preserve_their_distinct_at_behavior() {
        let mut input = Cursor::new(b"from\r\nstdin".to_vec());
        let options = parse(
            [
                OsString::from("-q"),
                OsString::from("-d"),
                OsString::from("@-"),
                OsString::from("--data-raw"),
                OsString::from("@literal"),
                OsString::from("http://example.test/"),
            ],
            &mut input,
        )
        .expect("data arguments should parse");
        assert_eq!(
            options.body.expect("data creates a body").bytes,
            b"fromstdin&@literal",
            "-d should strip file newlines while --data-raw treats @ literally"
        );
    }

    #[test]
    fn rejects_known_bad_hyphenless_spelling() {
        let error = parse(
            [
                OsString::from("-q"),
                OsString::from("--maxtime"),
                OsString::from("1"),
                OsString::from("http://example.test/"),
            ],
            &mut Cursor::new(Vec::new()),
        )
        .expect_err("the known-bad spelling must not be accepted");
        assert!(
            error.to_string().contains("unknown option: --maxtime"),
            "the parser should identify the rejected option"
        );
    }

    #[test]
    fn url_encoding_encodes_only_content_after_a_name() {
        let options = options(&[
            "--data-urlencode",
            "field=a value/+@host",
            "http://example.test/",
        ]);
        assert_eq!(
            options.body.expect("data creates a body").bytes,
            b"field=a%20value%2F%2B%40host",
            "the field name should remain literal and the value should be encoded"
        );
    }

    #[test]
    fn parses_network_tls_retry_and_diagnostic_options() {
        let options = options(&[
            "--connect-timeout",
            "1.5",
            "--max-time=3",
            "--retry",
            "4",
            "--retry-delay",
            "0.25",
            "--retry-max-time",
            "9",
            "--resolve",
            "example.test:8443:127.0.0.1",
            "--dns-servers",
            "127.0.0.1,[::1]:5353",
            "--ipv6",
            "--cacert",
            "ca.pem",
            "--cert",
            "client.pem",
            "--key",
            "key.pem",
            "--http2",
            "--tlsv1.3",
            "--compressed",
            "--no-keepalive",
            "--trace-ascii",
            "trace.txt",
            "--create-dirs",
            "--limit-rate",
            "2K",
            "http://example.test/",
        ]);
        assert_eq!(options.connect_timeout, Duration::from_millis(1500));
        assert_eq!(options.max_time, Some(Duration::from_secs(3)));
        assert_eq!(options.retry, 4);
        assert_eq!(options.resolve[0].addresses, [IpAddr::from([127, 0, 0, 1])]);
        assert_eq!(
            options.dns_servers[1],
            "[::1]:5353".parse().expect("valid address")
        );
        assert_eq!(options.ip_version, IpVersion::V6);
        assert_eq!(options.http_version, HttpVersion::Http2);
        assert_eq!(options.minimum_tls_version, MinimumTlsVersion::Tls13);
        assert!(options.compressed);
        assert!(!options.keepalive);
        assert_eq!(options.limit_rate, Some(2048));
    }

    #[test]
    fn builds_multipart_and_upload_bodies_distinctly() {
        let form = options(&["-F", "field=value", "http://example.test/"])
            .body
            .expect("form creates a body");
        assert_eq!(form.implied_method, "POST");
        assert!(
            form.content_type
                .expect("multipart has a content type")
                .starts_with(b"multipart/form-data; boundary="),
            "form content type should contain its boundary"
        );
        let body = String::from_utf8_lossy(&form.bytes);
        assert!(body.contains("name=\"field\"\r\n\r\nvalue\r\n"));

        let mut stdin = Cursor::new(b"upload bytes".to_vec());
        let upload = parse(
            [
                OsString::from("-q"),
                OsString::from("-T"),
                OsString::from("-"),
                OsString::from("http://example.test/"),
            ],
            &mut stdin,
        )
        .expect("upload should parse")
        .body
        .expect("upload creates a body");
        assert_eq!(upload.implied_method, "PUT");
        assert_eq!(upload.bytes, b"upload bytes");
    }

    #[test]
    fn multipart_boundary_does_not_collide_with_uploaded_bytes() {
        let candidate = format!("------------------------roxygen{:x}-0", std::process::id());
        let payload = format!("before\r\n--{candidate}\r\nafter");
        let mut stdin = Cursor::new(payload.as_bytes());
        let form = prepare_form(&["upload=@-".to_owned()], &mut stdin, &mut None)
            .expect("multipart upload should parse");
        let content_type = String::from_utf8(form.content_type.expect("multipart content type"))
            .expect("ASCII content type");
        let boundary = content_type
            .split_once("boundary=")
            .expect("boundary parameter")
            .1;
        assert_ne!(boundary, candidate);
        let body = String::from_utf8(form.bytes).expect("ASCII multipart body");
        assert!(body.contains(&payload), "upload must remain intact");
        assert_eq!(body.matches(&format!("--{boundary}")).count(), 2);
        assert_eq!(stdin.position(), payload.len() as u64);
    }

    #[test]
    fn zero_timeouts_match_curl_defaults() {
        let options = options(&[
            "--connect-timeout",
            "0",
            "--max-time",
            "0",
            "http://example.test/",
        ]);
        assert_eq!(options.connect_timeout, Duration::from_secs(300));
        assert_eq!(options.max_time, None);
    }

    #[test]
    fn multipart_file_honors_type_and_filename_attributes() {
        let mut stdin = Cursor::new(b"upload bytes".to_vec());
        let form = parse(
            [
                OsString::from("-q"),
                OsString::from("-F"),
                OsString::from("upload=@-;type=text/plain;filename=payload.txt"),
                OsString::from("http://example.test/"),
            ],
            &mut stdin,
        )
        .expect("multipart upload should parse")
        .body
        .expect("multipart upload creates a body");
        let body = String::from_utf8_lossy(&form.bytes);
        assert!(body.contains(
            "name=\"upload\"; filename=\"payload.txt\"\r\nContent-Type: text/plain\r\n\r\nupload bytes\r\n"
        ));
    }

    #[test]
    fn explicit_config_file_uses_curl_style_syntax() {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "roxygen-curl-config-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &path,
            "silent\nheader = \"X-Config: enabled\"\nurl = \"http://example.test/path\"\n",
        )
        .expect("test config should be written");
        let parsed = parse(
            [
                OsString::from("-q"),
                OsString::from("-K"),
                path.as_os_str().to_owned(),
            ],
            &mut Cursor::new(Vec::new()),
        )
        .expect("explicit config should parse");
        assert!(parsed.silent);
        assert_eq!(parsed.headers[0].name, b"X-Config");
        assert_eq!(parsed.transfers[0].url, "http://example.test/path");
        fs::remove_file(path).expect("test config should be removable");
    }

    #[test]
    fn no_forms_disable_previous_boolean_options() {
        let parsed = options(&[
            "--location",
            "--no-location",
            "--compressed",
            "--no-compressed",
            "--parallel",
            "--no-parallel",
            "http://example.test/",
        ]);
        assert!(!parsed.location);
        assert!(!parsed.compressed);
        assert!(!parsed.parallel);
    }
}
