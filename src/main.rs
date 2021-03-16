#[macro_use]
extern crate anyhow;

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use teloxide::utils::command::ParseError;
use tokio::io::Sink;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{mpsc, RwLock};
use warp::{http, Filter};

use settings::Settings;

use crate::listener::*;
use crate::state::*;

mod listener;
mod settings;
mod state;

fn json_body<T>() -> impl Filter<Extract = (T,), Error = warp::Rejection> + Clone
where
    for<'a> T: serde::Deserialize<'a> + Send,
{
    warp::body::content_length_limit(1024 * 1024).and(warp::filters::body::json::<T>())
}

fn parse_filter<'a, I>(mut tokens: I) -> Result<SubscriptionFilter, ParseError>
where
    I: Iterator<Item = &'a str>,
{
    let value_token = match tokens.next() {
        Some(token) if token == ">" || token == ">=" => tokens.next(),
        Some(_) => {
            return Err(ParseError::Custom(
                format!("unsupported filter, possible patterns are: `> AMOUNT`").into(),
            ));
        }
        None => return Ok(SubscriptionFilter::default()),
    };

    match value_token {
        Some(token) => match f64::from_str(token) {
            Ok(gt) => Ok(SubscriptionFilter {
                gt: Some((gt * 1000000000.0) as u64),
                ..Default::default()
            }),
            Err(e) => Err(ParseError::Custom(e.to_string().into())),
        },
        None => Ok(SubscriptionFilter::default()),
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Subscription {
    direction: Direction,
    address: String,
    filter: SubscriptionFilter,
    id: i64,
}

// fn parse_subscription(input: String) -> Result<(Subscription, ), ParseError> {
//     let mut parts = input.trim().split(' ').filter(|s| !s.is_empty());
//
//     let description = "Usage:\n    `/subscribe DIRECTION ADDRESS`\n\nWhere:\n    `DIRECTION` \\- `all`, `incoming`, `outgoing`\n    `ADDRESS` \\- raw or packed address";
//
//     match parts.next() {
//         Some(first) => match parts.next() {
//             Some(second) => match Direction::from_str(first) {
//                 Ok(direction) => Ok((Subscription {
//                     direction,
//                     address: second.to_string(),
//                     filter: parse_filter(parts)?,
//                 }, )),
//                 Err(e) => Err(ParseError::IncorrectFormat(
//                     format!("{}\n\n{}", e.to_string(), description).into(),
//                 )),
//             },
//             None => Ok((Subscription {
//                 direction: Direction::All,
//                 address: first.to_owned(),
//                 filter: Default::default(),
//             }, )),
//         },
//         None => Err(ParseError::Custom(description.to_owned().into())),
//     }
// }

struct SetComment {
    address: String,
    comment: String,
}

const ESCAPED_CHARACTERS: [char; 18] = [
    '_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!',
];

const ESCAPED_CHARACTERS_REPLACEMENT: [&str; 18] = [
    "\\_", "\\*", "\\[", "\\]", "\\(", "\\)", "\\~", "\\`", "\\>", "\\#", "\\+", "\\-", "\\=",
    "\\|", "\\{", "\\}", "\\.", "\\!",
];

fn parse_set_comment(input: String) -> Result<(SetComment,), ParseError> {
    fn escape_markdown(text: &str) -> String {
        let mut text = text.to_string();
        for (character, replacement) in ESCAPED_CHARACTERS
            .iter()
            .zip(ESCAPED_CHARACTERS_REPLACEMENT.iter())
        {
            text = text.replace(*character, replacement);
        }
        text
    }

    let description = "Usage:\n    `/setcomment ADDRESS COMMENT`\n\nWhere:\n    `ADDRESS` \\- raw or packed address\n    `COMMENT` \\- some notes, 40 characters max";

    let input = input.trim();
    match input.find(' ') {
        Some(space_pos) => {
            let (address, comment) = input.split_at(space_pos);

            let comment = {
                let trimmed_comment = comment.trim();
                if trimmed_comment.len() > 40 {
                    escape_markdown(&trimmed_comment[0..40]) + "\\.\\.\\."
                } else {
                    escape_markdown(trimmed_comment)
                }
            };

            Ok((SetComment {
                address: address.to_string(),
                comment,
            },))
        }
        None => Err(ParseError::Custom(description.into())),
    }
}

struct ClearComment {
    address: String,
}

fn parse_clear_comment(input: String) -> Result<(ClearComment,), ParseError> {
    Ok((ClearComment {
        address: input.trim().to_string(),
    },))
}

// #[derive(BotCommand)]
// #[command(rename = "lowercase", description = "These commands are supported:")]
// enum Command {
//     // #[command(description = "display this text")]
//     // Start,
//     // #[command(
//     // description = "subscribe to account",
//     // parse_with = "parse_subscription"
//     // )]
//     Subscribe(Subscription),
//     // #[command(
//     // description = "unsubscribe from account",
//     // parse_with = "parse_subscription"
//     // )]
//     Unsubscribe(Subscription),
//     // #[command(description = "set address comment", parse_with = "parse_set_comment")]
//     // SetComment(SetComment),
//     // #[command(
//     // description = "clear address comment",
//     // parse_with = "parse_clear_comment"
//     // )]
//     // ClearComment(ClearComment),
//     // #[command(description = "List subscriptions")]
//     // List,
// }

fn init_logger() {
    let log_filters = std::env::var("RUST_LOG").unwrap_or_default();

    pretty_env_logger::formatted_builder()
        .parse_filters(&log_filters)
        .format(|formatter, record| {
            use std::io::Write;
            writeln!(
                formatter,
                "{} [{}] - {}",
                chrono::Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),
                record.args()
            )
        })
        .init()
}

async fn run() -> anyhow::Result<()> {
    let settings = Settings::new()?;

    let id_channels = Arc::new(RwLock::new(HashMap::new()));

    let state = Arc::new(State::new(settings.db)?);

    let state_warp = warp::any().map({
        let st = state.clone();
        move || st.clone()
    });

    let id_channels_warp = warp::any().map({
        let id_ch = id_channels.clone();
        move || id_ch.clone()
    });

    let subscribe_route = warp::path!("subscribe")
        .and(warp::post())
        .and(state_warp.clone())
        .and(id_channels_warp.clone())
        .and(json_body::<Subscription>())
        .and_then(subscribe);

    let unsubscribe_route = warp::path!("unsubscribe")
        .and(warp::post())
        .and(state_warp.clone())
        .and(id_channels_warp.clone())
        .and(json_body::<Subscription>())
        .and_then(unsubscribe);

    let routes = warp::path!("stonks_alerts" / i64)
        .and(id_channels_warp.clone())
        .and(warp::ws())
        .map(
            |id: i64,
             id_channels: Arc<
                RwLock<HashMap<i64, (UnboundedSender<String>, UnboundedReceiver<String>)>>,
            >,
             ws: warp::ws::Ws| {
                ws.on_upgrade(move |socket| async move {
                    let mut tx = socket.split().0;
                    match id_channels.write().await.get_mut(&id) {
                        None => {}
                        Some((_tx, rx)) => {
                            while let Some(message) = rx.recv().await {
                                if let Err(e) = tx.send(warp::ws::Message::text(message)).await {
                                    log::error!("{}", e);
                                    break;
                                }
                            }
                        }
                    }
                })
            },
        );

    spawn_listener(settings.kafka, id_channels.clone(), state.clone())?;

    warp::serve(routes.or(subscribe_route).or(unsubscribe_route))
        .run(([127, 0, 0, 1], 3030))
        .await;

    Ok(())
}

async fn subscribe(
    state: Arc<State>,
    id_channels: Arc<RwLock<HashMap<i64, (UnboundedSender<String>, UnboundedReceiver<String>)>>>,
    subscription: Subscription,
) -> Result<impl warp::Reply, warp::Rejection> {
    match state.insert(
        &subscription.address,
        subscription.direction,
        subscription.filter,
        subscription.id,
    ) {
        Ok(_) => {
            id_channels
                .write()
                .await
                .insert(subscription.id, mpsc::unbounded_channel());
            log::info!(
                "Id \"{}\" subscribed to:\n`{}`",
                subscription.id,
                subscription.address
            );
            Ok(warp::reply::with_status(
                format!(
                    "Id \"{}\" subscribed from:\n`{}`",
                    subscription.id, subscription.address
                ),
                http::StatusCode::OK,
            ))
        }
        Err(e) => {
            log::error!("failed to subscribe: {:?}", e);
            Ok(warp::reply::with_status(
                format!("failed to unsubscribe: {:?}", e),
                http::StatusCode::OK,
            ))
        }
    }
}

async fn unsubscribe(
    state: Arc<State>,
    _id_channels: Arc<RwLock<HashMap<i64, (UnboundedSender<String>, UnboundedReceiver<String>)>>>,
    subscription: Subscription,
) -> Result<impl warp::Reply, warp::Rejection> {
    match state.remove(
        &subscription.address,
        subscription.direction,
        subscription.id,
    ) {
        Ok(_) => {
            // id_channels.write().await.remove(&subscription.id);
            log::info!(
                "Id \"{}\" unsubscribed from:\n`{}`",
                subscription.id,
                subscription.address
            );
            Ok(warp::reply::with_status(
                format!(
                    "Id \"{}\" unsubscribed from:\n`{}`",
                    subscription.id, subscription.address
                ),
                http::StatusCode::OK,
            ))
        }
        Err(e) => {
            log::error!("failed to unsubscribe: {:?}", e);
            Ok(warp::reply::with_status(
                format!("failed to unsubscribe: {:?}", e),
                http::StatusCode::OK,
            ))
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logger();
    run().await
}

// teloxide::repl(bot, move |cx| {
//     let state = state.clone();
//     let bot_name = bot_name.clone();
//
//     async move {
//         let chat_id = cx.update.chat_id();
//         let text = match cx.update.text() {
//             Some(text) => text,
//             None => return Ok(()),
//         };

// if cx.update.forward_from().is_some() || cx.update.reply_to_message().is_some() {
//     // don't react to forwards or replies
//     return Ok(());
// }

// match Command::parse(text, &bot_name) {
// Ok(Command::Start) => {
//     cx.answer(Command::descriptions()).send().await?;
// }
// Ok(Command::Subscribe(subscription)) => {
//     match state.insert(
//         &subscription.address,
//         subscription.direction,
//         subscription.filter,
//         chat_id,
//     ) {
//         Ok(_) => {
//             cx.reply_to(format!("Subscribed to:\n`{}`", subscription.address))
//                 .parse_mode(ParseMode::MarkdownV2)
//                 .send()
//                 .await?;
//         }
//         Err(e) => {
//             log::error!("failed to subscribe: {:?}", e);
//             cx.reply_to("Unable to subscribe to this address")
//                 .send()
//                 .await?;
//         }
//     }
// }
// Ok(Command::Unsubscribe(subscription)) => {
//     match state.remove(&subscription.address, subscription.direction, chat_id) {
//         Ok(_) => {
//             cx.reply_to(format!("Unsubscribed from:\n`{}`", subscription.address))
//                 .parse_mode(ParseMode::MarkdownV2)
//                 .send()
//                 .await?;
//         }
//         Err(e) => {
//             log::error!("failed to unsubscribe: {:?}", e);
//             cx.reply_to("Unable to unsubscribe from this address")
//                 .send()
//                 .await?;
//         }
//     }
// }
// Ok(Command::SetComment(data)) => {
//     match state.set_comment(chat_id, &data.address, &data.comment) {
//         Ok(_) => {
//             cx.reply_to("Done").send().await?;
//         }
//         Err(e) => {
//             log::error!("failed to set comment: {:?}", e);
//             cx.reply_to("Failed to set comment for this address")
//                 .send()
//                 .await?;
//         }
//     }
// }
// Ok(Command::ClearComment(data)) => {
//     match state.remove_comment(chat_id, &data.address) {
//         Ok(_) => {
//             cx.reply_to("Done").send().await?;
//         }
//         Err(e) => {
//             log::error!("failed to clear comment: {:?}", e);
//             cx.reply_to("Failed to clear comment for this address")
//                 .send()
//                 .await?;
//         }
//     }
// }
// Ok(Command::List) => {
//     let mut response = "Subscriptions:".to_owned();
//     for (i, (workchain, addr, direction, filter)) in
//     state.subscriptions(chat_id).enumerate()
//     {
//         let comment = state
//             .get_comment(chat_id, workchain, &addr)
//             .unwrap_or_default()
//             .unwrap_or_else(|| format!("{}\\. Unknown", i));
//
//         response += &format!(
//             "\n\n{} \\- {} {}\n`{}:{}`",
//             comment,
//             direction,
//             filter,
//             workchain,
//             hex::encode(&addr),
//         );
//     }

//     if let Err(e) = cx
//         .reply_to(response)
//         .parse_mode(ParseMode::MarkdownV2)
//         .send()
//         .await
//     {
//         log::error!("{}", e);
//     }
// }
// Err(e) => {
//     cx.reply_to(e.to_string())
//         .parse_mode(ParseMode::MarkdownV2)
//         .send()
//         .await?;
// };

// ResponseResult::Ok(())
// }
// })
//     .await;
//
// Ok(())
// }
