use crate::http::Endpoint;
use rmcp::{
    ErrorData, RoleClient, RoleServer, ServerHandler, ServiceExt,
    model::*,
    service::{RequestContext, RunningService},
    transport::{
        StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use std::{sync::Arc, time::Duration};

#[derive(Debug, thiserror::Error)]
pub enum ProxyError {
    #[error("local host is unavailable")]
    Unavailable,
    #[error("invalid local host endpoint")]
    InvalidEndpoint,
    #[error("MCP connection ended unexpectedly")]
    Interrupted,
}

struct Inner {
    client: RunningService<RoleClient, ClientConfig>,
    info: ServerConfig,
    tools: Vec<Tool>,
}
#[derive(Clone)]
pub struct McpProxy(Arc<Inner>);

impl McpProxy {
    pub async fn connect(endpoint: &Endpoint) -> Result<Self, ProxyError> {
        endpoint
            .validate()
            .map_err(|_| ProxyError::InvalidEndpoint)?;
        // This secret is only for the loopback host, regardless of system proxy settings.
        let client = crate::http::client_builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(1))
            .build()
            .map_err(|_| ProxyError::Unavailable)?;
        let transport = StreamableHttpClientTransport::with_client(
            client,
            StreamableHttpClientTransportConfig::with_uri(format!(
                "http://{}/mcp",
                endpoint.address
            ))
            .auth_header(&endpoint.token)
            .max_concurrent_requests(6),
        );
        let client = tokio::time::timeout(
            Duration::from_secs(5),
            ClientConfig::default().serve(transport),
        )
        .await
        .map_err(|_| ProxyError::Unavailable)?
        .map_err(|_| ProxyError::Unavailable)?;
        let peer = client.peer_info().ok_or(ProxyError::Unavailable)?;
        let mut info = ServerConfig::new(peer.capabilities.clone())
            .with_protocol_version(peer.protocol_version.clone())
            .with_server_info(
                peer.server_info
                    .clone()
                    .unwrap_or_else(|| Implementation::new("liazo", env!("CARGO_PKG_VERSION"))),
            );
        info.instructions = peer.instructions.clone();
        let tools = tokio::time::timeout(Duration::from_secs(5), client.list_all_tools())
            .await
            .map_err(|_| ProxyError::Unavailable)?
            .map_err(|_| ProxyError::Unavailable)?;
        Ok(Self(Arc::new(Inner {
            client,
            info,
            tools,
        })))
    }
}

impl ServerHandler for McpProxy {
    fn get_info(&self) -> ServerConfig {
        self.0.info.clone()
    }
    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.0.tools.iter().find(|tool| tool.name == name).cloned()
    }
    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        if request.is_some_and(|request| request.cursor.is_some()) {
            return Err(ErrorData::invalid_params("invalid tool cursor", None));
        }
        Ok(ListToolsResult::with_all_items(self.0.tools.clone()))
    }
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        tokio::select! {
            _ = context.ct.cancelled() => Err(ErrorData::internal_error("request cancelled", None)),
            result = self.0.client.call_tool(request) => match result {
                Ok(result) => Ok(result.into()),
                Err(rmcp::ServiceError::McpError(error)) => Err(error),
                Err(_) => Ok(CallToolResult::error(vec![ContentBlock::text("local host is unavailable")]).into()),
            },
        }
    }
}

pub async fn stdio(endpoint: &Endpoint) -> Result<(), ProxyError> {
    let running = McpProxy::connect(endpoint)
        .await?
        .serve(rmcp::transport::stdio())
        .await
        .map_err(|_| ProxyError::Interrupted)?;
    running
        .waiting()
        .await
        .map_err(|_| ProxyError::Interrupted)?;
    Ok(())
}
