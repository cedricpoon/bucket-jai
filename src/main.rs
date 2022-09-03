use std::{io, sync::Arc};

use actix_cors::Cors;
use actix_web::{
    get, middleware, route,
    web::{self, Data},
    App, HttpResponse, HttpServer, Responder,
};
use actix_web_lab::respond::Html;
use juniper::http::{graphiql::graphiql_source, GraphQLRequest};

mod gql;
mod redis;
mod hash;

use crate::gql::{create_schema, Schema};
use crate::redis::{redis_client, RedisCtx};

#[derive(Clone)]
struct Configuration {
    redis_address: String,
    server_address: String,
    server_port: u16
}

/// GraphiQL playground UI
#[get("/graphiql")]
async fn graphql_playground() -> impl Responder {
    Html(graphiql_source("/graphql", None))
}

/// GraphQL endpoint
#[route("/graphql", method = "GET", method = "POST")]
async fn graphql(st: web::Data<Schema>, cfg: web::Data<Configuration>, data: web::Json<GraphQLRequest>) -> impl Responder {
    if let Ok(_client) = redis_client(&cfg.redis_address) {
        let ctx = RedisCtx {
            client: _client
        };
        let resp = data.execute(&st, &ctx).await;
        return HttpResponse::Ok().json(resp);
    } else {
        return HttpResponse::ServiceUnavailable().body("Redis service not available");
    }
}

#[actix_web::main]
async fn main() -> io::Result<()> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    // Create Juniper schema
    let schema = Arc::new(create_schema());

    let config: Configuration = Configuration {
        redis_address: match std::env::var_os("REDIS_ADDR") {
            Some(v) => v.into_string().unwrap(),
            None => "redis://127.0.0.1/".to_string()
        },
        server_address: match std::env::var_os("SERVER_ADDR") {
            Some(v) => v.into_string().unwrap(),
            None => "127.0.0.1".to_string()
        },
        server_port: match std::env::var_os("SERVER_PORT") {
            Some(v) => v.into_string().unwrap().parse::<u16>().unwrap(),
            None => 8080
        },
    };

    log::info!("starting HTTP server on port {}", config.server_port);
    log::info!("GraphiQL playground: http://{}:{}/graphiql", config.server_address, config.server_port);
    log::info!("Redis address set to {}", config.redis_address);

    let addr = config.server_address.to_owned();
    let port = config.server_port;

    // Start HTTP server
    HttpServer::new(move || {
        App::new()
            .app_data(Data::from(schema.clone()))
            .app_data(Data::from(Arc::new(config.clone())))
            .service(graphql)
            .service(graphql_playground)
            // the graphiql UI requires CORS to be enabled
            .wrap(Cors::permissive())
            .wrap(middleware::Logger::default())
    })
    .bind((addr, port))?
    .run()
    .await
}