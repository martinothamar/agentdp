mod dns;
mod runtime;

pub(crate) use dns::{dns_a_response, dns_header, dns_query, push_dns_name, tcp_dns_frame, u16_len};
pub(crate) use runtime::runtime_context;
