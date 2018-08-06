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

const METHOD_IMPORT_SST_SWITCH_MODE: ::grpcio::Method<super::import_sstpb::SwitchModeRequest, super::import_sstpb::SwitchModeResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/import_sstpb.ImportSST/SwitchMode",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_IMPORT_SST_UPLOAD: ::grpcio::Method<super::import_sstpb::UploadRequest, super::import_sstpb::UploadResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ClientStreaming,
    name: "/import_sstpb.ImportSST/Upload",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_IMPORT_SST_INGEST: ::grpcio::Method<super::import_sstpb::IngestRequest, super::import_sstpb::IngestResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/import_sstpb.ImportSST/Ingest",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_IMPORT_SST_COMPACT: ::grpcio::Method<super::import_sstpb::CompactRequest, super::import_sstpb::CompactResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/import_sstpb.ImportSST/Compact",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

pub struct ImportSstClient {
    client: ::grpcio::Client,
}

impl ImportSstClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        ImportSstClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn switch_mode_opt(&self, req: &super::import_sstpb::SwitchModeRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::import_sstpb::SwitchModeResponse> {
        self.client.unary_call(&METHOD_IMPORT_SST_SWITCH_MODE, req, opt)
    }

    pub fn switch_mode(&self, req: &super::import_sstpb::SwitchModeRequest) -> ::grpcio::Result<super::import_sstpb::SwitchModeResponse> {
        self.switch_mode_opt(req, ::grpcio::CallOption::default())
    }

    pub fn switch_mode_async_opt(&self, req: &super::import_sstpb::SwitchModeRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::import_sstpb::SwitchModeResponse>> {
        self.client.unary_call_async(&METHOD_IMPORT_SST_SWITCH_MODE, req, opt)
    }

    pub fn switch_mode_async(&self, req: &super::import_sstpb::SwitchModeRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::import_sstpb::SwitchModeResponse>> {
        self.switch_mode_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn upload_opt(&self, opt: ::grpcio::CallOption) -> ::grpcio::Result<(::grpcio::ClientCStreamSender<super::import_sstpb::UploadRequest>, ::grpcio::ClientCStreamReceiver<super::import_sstpb::UploadResponse>)> {
        self.client.client_streaming(&METHOD_IMPORT_SST_UPLOAD, opt)
    }

    pub fn upload(&self) -> ::grpcio::Result<(::grpcio::ClientCStreamSender<super::import_sstpb::UploadRequest>, ::grpcio::ClientCStreamReceiver<super::import_sstpb::UploadResponse>)> {
        self.upload_opt(::grpcio::CallOption::default())
    }

    pub fn ingest_opt(&self, req: &super::import_sstpb::IngestRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::import_sstpb::IngestResponse> {
        self.client.unary_call(&METHOD_IMPORT_SST_INGEST, req, opt)
    }

    pub fn ingest(&self, req: &super::import_sstpb::IngestRequest) -> ::grpcio::Result<super::import_sstpb::IngestResponse> {
        self.ingest_opt(req, ::grpcio::CallOption::default())
    }

    pub fn ingest_async_opt(&self, req: &super::import_sstpb::IngestRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::import_sstpb::IngestResponse>> {
        self.client.unary_call_async(&METHOD_IMPORT_SST_INGEST, req, opt)
    }

    pub fn ingest_async(&self, req: &super::import_sstpb::IngestRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::import_sstpb::IngestResponse>> {
        self.ingest_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn compact_opt(&self, req: &super::import_sstpb::CompactRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::import_sstpb::CompactResponse> {
        self.client.unary_call(&METHOD_IMPORT_SST_COMPACT, req, opt)
    }

    pub fn compact(&self, req: &super::import_sstpb::CompactRequest) -> ::grpcio::Result<super::import_sstpb::CompactResponse> {
        self.compact_opt(req, ::grpcio::CallOption::default())
    }

    pub fn compact_async_opt(&self, req: &super::import_sstpb::CompactRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::import_sstpb::CompactResponse>> {
        self.client.unary_call_async(&METHOD_IMPORT_SST_COMPACT, req, opt)
    }

    pub fn compact_async(&self, req: &super::import_sstpb::CompactRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::import_sstpb::CompactResponse>> {
        self.compact_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item = (), Error = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait ImportSst {
    fn switch_mode(&self, ctx: ::grpcio::RpcContext, req: super::import_sstpb::SwitchModeRequest, sink: ::grpcio::UnarySink<super::import_sstpb::SwitchModeResponse>);
    fn upload(&self, ctx: ::grpcio::RpcContext, stream: ::grpcio::RequestStream<super::import_sstpb::UploadRequest>, sink: ::grpcio::ClientStreamingSink<super::import_sstpb::UploadResponse>);
    fn ingest(&self, ctx: ::grpcio::RpcContext, req: super::import_sstpb::IngestRequest, sink: ::grpcio::UnarySink<super::import_sstpb::IngestResponse>);
    fn compact(&self, ctx: ::grpcio::RpcContext, req: super::import_sstpb::CompactRequest, sink: ::grpcio::UnarySink<super::import_sstpb::CompactResponse>);
}

pub fn create_import_sst<S: ImportSst + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_IMPORT_SST_SWITCH_MODE, move |ctx, req, resp| {
        instance.switch_mode(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_client_streaming_handler(&METHOD_IMPORT_SST_UPLOAD, move |ctx, req, resp| {
        instance.upload(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_IMPORT_SST_INGEST, move |ctx, req, resp| {
        instance.ingest(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_IMPORT_SST_COMPACT, move |ctx, req, resp| {
        instance.compact(ctx, req, resp)
    });
    builder.build()
}
