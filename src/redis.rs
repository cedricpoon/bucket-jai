use redis::Commands;

pub const HASH_KF_MIME : &str = "mime";
pub const HASH_KF_DATA : &str = "data";
pub const HASH_KF_RSA : &str = "rsa";

pub struct RedisCtx {
    pub client: redis::Client
}

pub enum Status { Int(isize), String(String) }

pub enum RedisRtn {
    Payload(String),
    Status(Status),
    Payloads(Vec<String>)
}

pub fn str_k_slang(s : &String) -> String { format!("idx:str:slg:{}", s) }

pub fn hash_k_id(s : &String) -> String { format!("bkt:hash:id:{}", s) }

pub fn zset_k_id(s : &String) -> String { format!("slgs:zset:id:{}", s) }

pub fn redis_client(redis_addr: &String) -> Result<redis::Client, redis::RedisError> {
    Ok(redis::Client::open(redis_addr.to_owned())?)
}

pub fn get_kv(client: &redis::Client, key: &String) -> redis::RedisResult<Option<RedisRtn>> {
    let mut con = client.get_connection()?;

    let value = con.get(&key)?;
    match value {
        None => {
            log::warn!("Redis does not have key {}", &key);
            Ok(None)
        },
        Some(x) => {
            log::info!("Redis get ([key] {}; [value] {})", &key, x);
            Ok(Some(RedisRtn::Payload(x)))
        }
    }
}

pub fn set_kv(client: &redis::Client, key: &String, value: &String) -> redis::RedisResult<RedisRtn> {
    let mut con = client.get_connection()?;

    let result = con.set(&key, &value)?;
    log::info!("Redis set ([key] {}; [value] {})", &key, &value);
    Ok(RedisRtn::Status(Status::String(result)))
}

pub fn del_kv(client: &redis::Client, key: &String) -> redis::RedisResult<RedisRtn> {
    let mut con = client.get_connection()?;

    let result = con.del(&key)?;
    log::info!("Redis delete ([key] {})", &key);
    Ok(RedisRtn::Status(Status::Int(result)))
}

pub fn get_hash_kfv(client: &redis::Client, id: &String, key: &String) -> redis::RedisResult<Option<RedisRtn>> {
    let mut con = client.get_connection()?;

    let value = con.hget(&id, &key)?;
    match value {
        None => {
            log::warn!("Redis hash does not have id {} key {}", &id, &key);
            Ok(None)
        },
        Some(x) => {
            log::info!("Redis hash get ([id] {}; [key] {}; [value] {})", &id, &key, x);
            Ok(Some(RedisRtn::Payload(x)))
        }
    }
}

pub fn set_hash_kfv(client: &redis::Client, id: &String, key: &String, value: &String) -> redis::RedisResult<RedisRtn> {
    let mut con = client.get_connection()?;

    let result = con.hset(&id, &key, &value)?;
    log::info!("Redis hash set ([id] {}; [key] {}; [value] {})", &id, &key, &value);
    Ok(RedisRtn::Status(Status::Int(result)))
}

pub fn set_sorted_kvs(client: &redis::Client, key: &String, value: &String, score: isize) -> redis::RedisResult<RedisRtn> {
    let mut con = client.get_connection()?;

    let result = con.zadd(&key, &value, score)?;
    log::info!("Redis zset set ([key] {}; [value] {}; [score] {})", &key, &value, score);
    Ok(RedisRtn::Status(Status::Int(result)))
}

pub fn del_sorted_kvs(client: &redis::Client, key: &String, member: &String) -> redis::RedisResult<RedisRtn> {
    let mut con = client.get_connection()?;

    let result = con.zrem(&key, &member)?;
    log::info!("Redis zset delete ([key] {}; [member] {})", &key, &member);
    Ok(RedisRtn::Status(Status::Int(result)))
}

pub fn get_sorted_kv(client: &redis::Client, key: &String) -> redis::RedisResult<Option<RedisRtn>> {
    let mut con = client.get_connection()?;

    let value : Option<Vec<String>> = con.zrange(&key, 0, -1)?;
    match value {
        None => {
            log::warn!("Redis zset does not have key {}", &key);
            Ok(None)
        },
        Some(x) => {
            log::info!("Redis zset get ([key] {}; [values] {})", &key, x.join(","));
            Ok(Some(RedisRtn::Payloads(x)))
        }
    }
}