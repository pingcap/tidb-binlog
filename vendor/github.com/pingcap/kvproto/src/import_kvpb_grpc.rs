// This file is generated. Do not edit
// @generated

// https://github.com/Manishearth/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy)]

#![cfg_attr(rustfmt, rustfmt_skip)]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unsafe_code)]
#![allow(unused_imports)]
#![allow(unused_results)]

const METHOD_IMPORT_KV_SWITCH_MODE: ::grpcio::Method<super::import_kvpb::SwitchModeRequest, super::import_kvpb::SwitchModeResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/import_kvpb.ImportKV/SwitchMode",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_IMPORT_KV_OPEN_ENGINE: ::grpcio::Method<super::import_kvpb::OpenEngineRequest, super::import_kvpb::OpenEngineResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/import_kvpb.ImportKV/OpenEngine",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_IMPORT_KV_WRITE_ENGINE: ::grpcio::Method<super::import_kvpb::WriteEngineRequest, super::import_kvpb::WriteEngineResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ClientStreaming,
    name: "/import_kvpb.ImportKV/WriteEngine",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_IMPORT_KV_CLOSE_ENGINE: ::grpcio::Method<super::import_kvpb::CloseEngineRequest, super::import_kvpb::CloseEngineResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/import_kvpb.ImportKV/CloseEngine",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_IMPORT_KV_IMPORT_ENGINE: ::grpcio::Method<super::import_kvpb::ImportEngineRequest, super::import_kvpb::ImportEngineResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/import_kvpb.ImportKV/ImportEngine",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_IMPORT_KV_CLEANUP_ENGINE: ::grpcio::Method<super::import_kvpb::CleanupEngineRequest, super::import_kvpb::CleanupEngineResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/import_kvpb.ImportKV/CleanupEngine",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_IMPORT_KV_COMPACT_CLUSTER: ::grpcio::Method<super::import_kvpb::CompactClusterRequest, super::import_kvpb::CompactClusterResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/import_kvpb.ImportKV/CompactCluster",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

pub struct ImportKvClient {
    client: ::grpcio::Client,
}

impl ImportKvClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        ImportKvClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn switch_mode_opt(&self, req: &super::import_kvpb::SwitchModeRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::import_kvpb::SwitchModeResponse> {
        self.client.unary_call(&METHOD_IMPORT_KV_SWITCH_MODE, req, opt)
    }

    pub fn switch_mode(&self, req: &super::import_kvpb::SwitchModeRequest) -> ::grpcio::Result<super::import_kvpb::SwitchModeResponse> {
        self.switch_mode_opt(req, ::grpcio::CallOption::default())
    }

    pub fn switch_mode_async_opt(&self, req: &super::import_kvpb::SwitchModeRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::import_kvpb::SwitchModeResponse>> {
        self.client.unary_call_async(&METHOD_IMPORT_KV_SWITCH_MODE, req, opt)
    }

    pub fn switch_mode_async(&self, req: &super::import_kvpb::SwitchModeRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::import_kvpb::SwitchModeResponse>> {
        self.switch_mode_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn open_engine_opt(&self, req: &super::import_kvpb::OpenEngineRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::import_kvpb::OpenEngineResponse> {
        self.client.unary_call(&METHOD_IMPORT_KV_OPEN_ENGINE, req, opt)
    }

    pub fn open_engine(&self, req: &super::import_kvpb::OpenEngineRequest) -> ::grpcio::Result<super::import_kvpb::OpenEngineResponse> {
        self.open_engine_opt(req, ::grpcio::CallOption::default())
    }

    pub fn open_engine_async_opt(&self, req: &super::import_kvpb::OpenEngineRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::import_kvpb::OpenEngineResponse>> {
        self.client.unary_call_async(&METHOD_IMPORT_KV_OPEN_ENGINE, req, opt)
    }

    pub fn open_engine_async(&self, req: &super::import_kvpb::OpenEngineRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::import_kvpb::OpenEngineResponse>> {
        self.open_engine_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn write_engine_opt(&self, opt: ::grpcio::CallOption) -> ::grpcio::Result<(::grpcio::ClientCStreamSender<super::import_kvpb::WriteEngineRequest>, ::grpcio::ClientCStreamReceiver<super::import_kvpb::WriteEngineResponse>)> {
        self.client.client_streaming(&METHOD_IMPORT_KV_WRITE_ENGINE, opt)
    }

    pub fn write_engine(&self) -> ::grpcio::Result<(::grpcio::ClientCStreamSender<super::import_kvpb::WriteEngineRequest>, ::grpcio::ClientCStreamReceiver<super::import_kvpb::WriteEngineResponse>)> {
        self.write_engine_opt(::grpcio::CallOption::default())
    }

    pub fn close_engine_opt(&self, req: &super::import_kvpb::CloseEngineRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::import_kvpb::CloseEngineResponse> {
        self.client.unary_call(&METHOD_IMPORT_KV_CLOSE_ENGINE, req, opt)
    }

    pub fn close_engine(&self, req: &super::import_kvpb::CloseEngineRequest) -> ::grpcio::Result<super::import_kvpb::CloseEngineResponse> {
        self.close_engine_opt(req, ::grpcio::CallOption::default())
    }

    pub fn close_engine_async_opt(&self, req: &super::import_kvpb::CloseEngineRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::import_kvpb::CloseEngineResponse>> {
        self.client.unary_call_async(&METHOD_IMPORT_KV_CLOSE_ENGINE, req, opt)
    }

    pub fn close_engine_async(&self, req: &super::import_kvpb::CloseEngineRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::import_kvpb::CloseEngineResponse>> {
        self.close_engine_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn import_engine_opt(&self, req: &super::import_kvpb::ImportEngineRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::import_kvpb::ImportEngineResponse> {
        self.client.unary_call(&METHOD_IMPORT_KV_IMPORT_ENGINE, req, opt)
    }

    pub fn import_engine(&self, req: &super::import_kvpb::ImportEngineRequest) -> ::grpcio::Result<super::import_kvpb::ImportEngineResponse> {
        self.import_engine_opt(req, ::grpcio::CallOption::default())
    }

    pub fn import_engine_async_opt(&self, req: &super::import_kvpb::ImportEngineRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::import_kvpb::ImportEngineResponse>> {
        self.client.unary_call_async(&METHOD_IMPORT_KV_IMPORT_ENGINE, req, opt)
    }

    pub fn import_engine_async(&self, req: &super::import_kvpb::ImportEngineRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::import_kvpb::ImportEngineResponse>> {
        self.import_engine_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn cleanup_engine_opt(&self, req: &super::import_kvpb::CleanupEngineRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::import_kvpb::CleanupEngineResponse> {
        self.client.unary_call(&METHOD_IMPORT_KV_CLEANUP_ENGINE, req, opt)
    }

    pub fn cleanup_engine(&self, req: &super::import_kvpb::CleanupEngineRequest) -> ::grpcio::Result<super::import_kvpb::CleanupEngineResponse> {
        self.cleanup_engine_opt(req, ::grpcio::CallOption::default())
    }

    pub fn cleanup_engine_async_opt(&self, req: &super::import_kvpb::CleanupEngineRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::import_kvpb::CleanupEngineResponse>> {
        self.client.unary_call_async(&METHOD_IMPORT_KV_CLEANUP_ENGINE, req, opt)
    }

    pub fn cleanup_engine_async(&self, req: &super::import_kvpb::CleanupEngineRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::import_kvpb::CleanupEngineResponse>> {
        self.cleanup_engine_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn compact_cluster_opt(&self, req: &super::import_kvpb::CompactClusterRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::import_kvpb::CompactClusterResponse> {
        self.client.unary_call(&METHOD_IMPORT_KV_COMPACT_CLUSTER, req, opt)
    }

    pub fn compact_cluster(&self, req: &super::import_kvpb::CompactClusterRequest) -> ::grpcio::Result<super::import_kvpb::CompactClusterResponse> {
        self.compact_cluster_opt(req, ::grpcio::CallOption::default())
    }

    pub fn compact_cluster_async_opt(&self, req: &super::import_kvpb::CompactClusterRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::import_kvpb::CompactClusterResponse>> {
        self.client.unary_call_async(&METHOD_IMPORT_KV_COMPACT_CLUSTER, req, opt)
    }

    pub fn compact_cluster_async(&self, req: &super::import_kvpb::CompactClusterRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::import_kvpb::CompactClusterResponse>> {
        self.compact_cluster_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item = (), Error = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait ImportKv {
    fn switch_mode(&self, ctx: ::grpcio::RpcContext, req: super::import_kvpb::SwitchModeRequest, sink: ::grpcio::UnarySink<super::import_kvpb::SwitchModeResponse>);
    fn open_engine(&self, ctx: ::grpcio::RpcContext, req: super::import_kvpb::OpenEngineRequest, sink: ::grpcio::UnarySink<super::import_kvpb::OpenEngineResponse>);
    fn write_engine(&self, ctx: ::grpcio::RpcContext, stream: ::grpcio::RequestStream<super::import_kvpb::WriteEngineRequest>, sink: ::grpcio::ClientStreamingSink<super::import_kvpb::WriteEngineResponse>);
    fn close_engine(&self, ctx: ::grpcio::RpcContext, req: super::import_kvpb::CloseEngineRequest, sink: ::grpcio::UnarySink<super::import_kvpb::CloseEngineResponse>);
    fn import_engine(&self, ctx: ::grpcio::RpcContext, req: super::import_kvpb::ImportEngineRequest, sink: ::grpcio::UnarySink<super::import_kvpb::ImportEngineResponse>);
    fn cleanup_engine(&self, ctx: ::grpcio::RpcContext, req: super::import_kvpb::CleanupEngineRequest, sink: ::grpcio::UnarySink<super::import_kvpb::CleanupEngineResponse>);
    fn compact_cluster(&self, ctx: ::grpcio::RpcContext, req: super::import_kvpb::CompactClusterRequest, sink: ::grpcio::UnarySink<super::import_kvpb::CompactClusterResponse>);
}

pub fn create_import_kv<S: ImportKv + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_IMPORT_KV_SWITCH_MODE, move |ctx, req, resp| {
        instance.switch_mode(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_IMPORT_KV_OPEN_ENGINE, move |ctx, req, resp| {
        instance.open_engine(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_client_streaming_handler(&METHOD_IMPORT_KV_WRITE_ENGINE, move |ctx, req, resp| {
        instance.write_engine(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_IMPORT_KV_CLOSE_ENGINE, move |ctx, req, resp| {
        instance.close_engine(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_IMPORT_KV_IMPORT_ENGINE, move |ctx, req, resp| {
        instance.import_engine(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_IMPORT_KV_CLEANUP_ENGINE, move |ctx, req, resp| {
        instance.cleanup_engine(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_IMPORT_KV_COMPACT_CLUSTER, move |ctx, req, resp| {
        instance.compact_cluster(ctx, req, resp)
    });
    builder.build()
}
