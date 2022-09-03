use juniper::FieldResult;
use juniper::graphql_value;
use juniper::{EmptySubscription, RootNode};
use juniper::{GraphQLInputObject, GraphQLObject};
use std::io::{Error, ErrorKind};

use crate::hash::*;
use crate::redis::*;

#[derive(GraphQLObject)]
#[graphql(description = "Bucket")]
struct Bucket {
    bucket_context: BucketContext,
    bucket_meta: BucketMeta
}

#[derive(GraphQLObject)]
#[graphql(description = "Bucket context")]
struct BucketContext {
    id:   String,   // SHA-256 of context
    data: String,   // data, in string
    mime: String    // mime of data
}

#[derive(GraphQLObject)]
#[graphql(description = "Metadata for bucket")]
struct BucketMeta {
    id:     String,         // SHA-256 of context
    slang:  Vec<String>,   // queryable slang for BucketContext
    rsa:    Option<String>, // RSA public key for encryption at rest
}

#[derive(GraphQLInputObject)]
#[graphql(description = "New bucket")]
struct NewBucket {
    data:   String,
    mime:   String,
    rsa:    Option<String> 
}

#[derive(GraphQLInputObject)]
#[graphql(description = "Update Bucket Metadata")]
struct MetaChange {
    id:     String,
    slang:  String
}

impl juniper::Context for RedisCtx {}

pub struct QueryRoot;

fn query_bucket_context(context: &RedisCtx, slang: String) -> FieldResult<BucketContext> {
    // get id
    let _id_res = get_kv(&context.client, &str_k_slang(&slang));
    if let Err(e) = _id_res { return Err(new_field_error(Box::new(e), "GET_STR_K_SLANG")) }
    if let None = _id_res.as_ref().unwrap() { return Err(new_field_error(Box::new(Error::from(ErrorKind::NotFound)), "NO_SLANG")) }
    let _id = if let Some(RedisRtn::Payload(_id)) = _id_res.as_ref().unwrap() { _id.to_owned() } else { "".to_owned() };

    // get data
    let _data_res = get_hash_kfv(&context.client, &hash_k_id(&_id), &HASH_KF_DATA.to_string());
    if let Err(e) = _data_res { return Err(new_field_error(Box::new(e), "GET_HASH_KF_DATA")) }
    let _data = if let Some(RedisRtn::Payload(x)) = _data_res.unwrap() { x.to_owned() } else { "".to_owned() };

    // get mime
    let _mime_res = get_hash_kfv(&context.client, &hash_k_id(&_id), &HASH_KF_MIME.to_string());
    if let Err(e) = _mime_res { return Err(new_field_error(Box::new(e), "GET_HASH_KF_MIME")) }
    let _mime = if let Some(RedisRtn::Payload(x)) = _mime_res.unwrap() { x.to_owned() } else { "".to_owned() };

    Ok(BucketContext {
        id: _id,
        data: _data,
        mime: _mime
    })
}

fn query_bucket_meta(context: &RedisCtx, slang: String) -> FieldResult<BucketMeta> {
    // get id
    let _id_res = get_kv(&context.client, &str_k_slang(&slang));
    if let Err(e) = _id_res { return Err(new_field_error(Box::new(e), "GET_STR_K_SLANG")) }
    if let None = _id_res.as_ref().unwrap() { return Err(new_field_error(Box::new(Error::from(ErrorKind::NotFound)), "NO_SLANG")) }
    let _id = if let Some(RedisRtn::Payload(_id)) = _id_res.as_ref().unwrap() { _id.to_owned() } else { "".to_owned() };

    // get slangs
    let _slang_res = get_sorted_kv(&context.client, &zset_k_id(&_id));
    if let Err(e) = _slang_res { return Err(new_field_error(Box::new(e), "GET_ZSET_K_ID")) }
    let _slang = if let Some(RedisRtn::Payloads(_slang)) = _slang_res.unwrap() { _slang } else { Vec::new() };

    // get rsa
    let _rsa_res = get_hash_kfv(&context.client, &hash_k_id(&_id), &HASH_KF_RSA.to_string());
    if let Err(e) = _rsa_res { return Err(new_field_error(Box::new(e), "GET_HASH_KF_RSA")) }
    let _option_rsa = if let Some(RedisRtn::Payload(_rsa)) = _rsa_res.unwrap() { Some(_rsa) } else { None };

    Ok(BucketMeta {
        id: _id,
        slang: _slang.to_owned(),
        rsa: _option_rsa
    })
}

#[juniper::graphql_object(context = RedisCtx)]
impl QueryRoot {
    fn apiVersion() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }

    pub fn bucketContext(context: &RedisCtx, slang: String) -> FieldResult<BucketContext> {
        query_bucket_context(context, slang)
    }

    pub fn bucketMeta(context: &RedisCtx, slang: String) -> FieldResult<BucketMeta> {
        query_bucket_meta(context, slang)
    }
}

pub struct MutationRoot;

fn new_field_error(e: Box<dyn std::error::Error>, message: &str) -> juniper::FieldError {
    log::error!("{}", e);
    juniper::FieldError::new(
        &message,
        graphql_value!({ "internal_error": message })
    )
}

fn set_slang(context: &RedisCtx, meta_change: &MetaChange) -> Result<(), juniper::FieldError> {
    // str: slang -> id
    if let Err(e) = set_kv(&context.client, &str_k_slang(&meta_change.slang), &meta_change.id) {
        return Err(new_field_error(Box::new(e), "SET_STR_K_SLANG")); 
    }
    // zset: id -> slang
    if let Err(e) = set_sorted_kvs(&context.client, &zset_k_id(&meta_change.id), &meta_change.slang, 0) {
        return Err(new_field_error(Box::new(e), "SET_ZSET_K_ID")); 
    }
    Ok(())
}

#[juniper::graphql_object(context = RedisCtx)]
impl MutationRoot {
    fn deleteBucket(context: &RedisCtx, id: String) -> FieldResult<Bucket> {
        // get slangs
        let _slang_res = get_sorted_kv(&context.client, &zset_k_id(&id));
        if let Err(e) = _slang_res { return Err(new_field_error(Box::new(e), "GET_ZSET_K_ID")) }
        let _slang = if let Some(RedisRtn::Payloads(_slang)) = _slang_res.unwrap() { _slang } else { Vec::new() };
        if _slang.len() == 0 { return Err(new_field_error(Box::new(Error::from(ErrorKind::NotFound)), "NO_ID"))  }

        let _bc = query_bucket_context(context, _slang[0].to_owned());
        if let Err(e) = _bc { return Err(e) }
        let _bm = query_bucket_meta(context, _slang[0].to_owned());
        if let Err(e) = _bm { return Err(e) }

        // del hash
        if let Err(e) = del_kv(&context.client, &hash_k_id(&id)) {
            return Err(new_field_error(Box::new(e), "DEL_HASH_K_ID")); 
        }
        // del each slang
        for x in _slang {
            if let Err(e) = del_kv(&context.client, &str_k_slang(&x)) {
                return Err(new_field_error(Box::new(e), "DEL_STR_K_SLANG")); 
            }
        }
        // del zset
        if let Err(e) = del_kv(&context.client, &zset_k_id(&id)) {
            return Err(new_field_error(Box::new(e), "DEL_ZSET_K_ID")); 
        }
        
        Ok(Bucket {
            bucket_context: _bc.unwrap(),
            bucket_meta: _bm.unwrap()
        })
    }

    fn dropSlang(context: &RedisCtx, meta_change: MetaChange) -> FieldResult<BucketMeta> {
        // get slangs
        let _slang_res_bef = get_sorted_kv(&context.client, &zset_k_id(&meta_change.id));
        if let Err(e) = _slang_res_bef { return Err(new_field_error(Box::new(e), "GET_ZSET_K_ID")) }
        if let Some(RedisRtn::Payloads(_slang)) = _slang_res_bef.as_ref().unwrap() {
            if _slang.len() == 1 && _slang[0].eq(&meta_change.slang) {
                return Err(new_field_error(Box::new(Error::from(ErrorKind::Unsupported)), "ID_LAST_SLANG")) 
            }
        }
        if let None = _slang_res_bef.as_ref().unwrap() { return Err(new_field_error(Box::new(Error::from(ErrorKind::NotFound)), "NO_ID")) }

        // try get id by slang
        let _id_res = get_kv(&context.client, &str_k_slang(&meta_change.slang));
        if let Err(e) = _id_res { return Err(new_field_error(Box::new(e), "GET_STR_K_SLANG")) }
        if let Some(RedisRtn::Payload(_id)) = _id_res.unwrap() {
            if !_id.eq(&meta_change.id) {
                return Err(new_field_error(Box::new(Error::from(ErrorKind::Unsupported)), "ID_SLANG_MISMATCH")) 
            }
        }

        // del slang -> id
        if let Err(e) = del_kv(&context.client, &str_k_slang(&meta_change.slang)) {
            return Err(new_field_error(Box::new(e), "DEL_STR_K_SLANG")); 
        }

        // del zset: id -> slang
        if let Err(e) = del_sorted_kvs(&context.client, &zset_k_id(&meta_change.id), &meta_change.slang) {
            return Err(new_field_error(Box::new(e), "DEL_ZSET_K_ID")); 
        }

        // try get rsa by id
        let _rsa_res = get_hash_kfv(&context.client, &hash_k_id(&meta_change.id), &HASH_KF_RSA.to_string());
        if let Err(e) = _rsa_res { return Err(new_field_error(Box::new(e), "GET_HASH_KF_RSA")) }
        let _option_rsa = if let Some(RedisRtn::Payload(_rsa)) = _rsa_res.unwrap() { Some(_rsa) } else { None };

        // get slangs
        let _slang_res = get_sorted_kv(&context.client, &zset_k_id(&meta_change.id));
        if let Err(e) = _slang_res { return Err(new_field_error(Box::new(e), "GET_ZSET_K_ID")) }
        let _slang = if let Some(RedisRtn::Payloads(_slang)) = _slang_res.unwrap() { _slang } else { Vec::new() };

        Ok(BucketMeta {
            id: meta_change.id,
            slang: _slang,
            rsa: _option_rsa
        })
    }

    fn setSlang(context: &RedisCtx, meta_change: MetaChange) -> FieldResult<BucketMeta> {
        // get mime
        let _mime_res = get_hash_kfv(&context.client, &hash_k_id(&meta_change.id), &HASH_KF_MIME.to_string());
        if let Err(e) = _mime_res { return Err(new_field_error(Box::new(e), "GET_HASH_KF_MIME")) }
        if let None = _mime_res.as_ref().unwrap() { return Err(new_field_error(Box::new(Error::from(ErrorKind::NotFound)), "NO_ID")) }

        // try get id by slang
        let _id_res = get_kv(&context.client, &str_k_slang(&meta_change.slang));
        if let Err(e) = _id_res { return Err(new_field_error(Box::new(e), "GET_STR_K_SLANG")) }
        if let Some(_) = _id_res.unwrap() { return Err(new_field_error(Box::new(Error::from(ErrorKind::AlreadyExists)), "SLANG_EXISTS")) }

        // id slang processing
        if let Err(e) = set_slang(&context, &meta_change) {
            return Err(e);
        }

        // try get rsa by id
        let _rsa_res = get_hash_kfv(&context.client, &hash_k_id(&meta_change.id), &HASH_KF_RSA.to_string());
        if let Err(e) = _rsa_res { return Err(new_field_error(Box::new(e), "GET_HASH_KF_RSA")) }
        let _option_rsa = if let Some(RedisRtn::Payload(_rsa)) = _rsa_res.unwrap() { Some(_rsa) } else { None };

        // get slangs
        let _slang_res = get_sorted_kv(&context.client, &zset_k_id(&meta_change.id));
        if let Err(e) = _slang_res { return Err(new_field_error(Box::new(e), "GET_ZSET_K_ID")) }
        let _slang = if let Some(RedisRtn::Payloads(_slang)) = _slang_res.unwrap() { _slang } else { Vec::new() };

        Ok(BucketMeta {
            id: meta_change.id,
            slang: _slang,
            rsa: _option_rsa
        })
    }

    fn createBucket(context: &RedisCtx, new_bucket: NewBucket) -> FieldResult<BucketMeta> {
        //TODO: Validation

        let _id = get_id_from_context(&new_bucket.data);
        let _slang = get_slang_from_id(&_id);
        
        //TODO: Centralized error handling and rollback
        // Add Bucket
        // hash: mime
        if let Err(e) = set_hash_kfv(&context.client, &hash_k_id(&_id), &HASH_KF_MIME.to_string(), &new_bucket.mime) {
            return Err(new_field_error(Box::new(e), "SET_HASH_KF_MIME")); 
        }
        // hash: data
        if let Err(e) = set_hash_kfv(&context.client, &hash_k_id(&_id), &HASH_KF_DATA.to_string(), &new_bucket.data) {
            return Err(new_field_error(Box::new(e), "SET_HASH_KF_DATA")); 
        }
        // id slang processing
        if let Err(e) = set_slang(&context, &MetaChange { id: _id.to_owned(), slang: _slang.to_owned() }) {
            return Err(e);
        }
        if let Some(ref _rsa) = new_bucket.rsa {
            //TODO: RSA public key encryption for data
            // hash: rsa
            if let Err(e) = set_hash_kfv(&context.client, &hash_k_id(&_id), &HASH_KF_RSA.to_string(), &_rsa) {
                return Err(new_field_error(Box::new(e), "SET_HASH_KF_RSA")); 
            }
        }

        // Get slangs
        let _slang_res = get_sorted_kv(&context.client, &zset_k_id(&_id));
        if let Err(e) = _slang_res { return Err(new_field_error(Box::new(e), "GET_ZSET_K_ID")) }
        let _slang = if let Some(RedisRtn::Payloads(_slang)) = _slang_res.unwrap() { _slang } else { Vec::new() };

        Ok(BucketMeta {
            id: _id.to_owned(),
            slang: _slang,
            rsa: new_bucket.rsa
        })
    }
}

pub type Schema = RootNode<'static, QueryRoot, MutationRoot, EmptySubscription<RedisCtx>>;

pub fn create_schema() -> Schema {
    Schema::new(QueryRoot {}, MutationRoot {}, EmptySubscription::new())
}