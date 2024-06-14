use std::{collections::HashSet, io::Write, time::Duration};

use chrono::{TimeZone, Utc};
use serde::{de, Deserialize, Deserializer, Serialize};
use serde_json::Value;

const USER_AGENT: &str = "node_counter_agent";
const MAINNET_GRAPHQL_URL: &str = "https://graphql.grid.tf/graphql";

const NODE_QUERY: &str = r#"
query MyQuery {  nodes {    nodeID    created    farmID    resourcesTotal {      cru      hru      mru      sru    }  }}
"#;

const START_YEAR: i32 = 2022;

#[tokio::main]
async fn main() {
    let client = reqwest::ClientBuilder::new()
        .user_agent(USER_AGENT)
        .gzip(true)
        .timeout(Duration::from_secs(30))
        .build()
        .expect("Client config is valid");

    let nodes = client
        .post(MAINNET_GRAPHQL_URL)
        .json(&GraphQLRequest::<()> {
            operation_name: "list_nodes",
            query: NODE_QUERY,
            variables: None,
        })
        .send()
        .await
        .expect("Can send a graphql request and get a reply")
        .json::<GraphQLResponse<NodeReply>>()
        .await
        .expect("Can parse response");

    let mut file = std::fs::File::create("node_count.csv").expect("Can create file");

    // header
    writeln!(
        file,
        "date,node count,farms with nodes,total CRU,total MRU,total SRU,total HRU"
    )
    .unwrap();

    // 10 years should be good
    for year in START_YEAR..START_YEAR + 10 {
        for month in 1..=12 {
            let start = Utc
                .with_ymd_and_hms(year, month, 1, 0, 0, 0)
                .unwrap()
                .timestamp();

            if Utc::now().timestamp() < start {
                break;
            };

            let (node_count, farms, total_resources) = nodes
                .data
                .nodes
                .iter()
                .filter(|node| node.created < start)
                .fold(
                    (
                        0,
                        HashSet::new(),
                        Resources {
                            cru: 0,
                            mru: 0,
                            sru: 0,
                            hru: 0,
                        },
                    ),
                    |(node_count, mut farms, mut resources), node| {
                        farms.insert(node.farm_id);
                        resources.cru += node.resources_total.cru;
                        resources.mru += node.resources_total.mru;
                        resources.hru += node.resources_total.hru;
                        resources.sru += node.resources_total.sru;
                        (node_count + 1, farms, resources)
                    },
                );

            writeln!(
                file,
                "{year}-{month}-1,{node_count},{},{},{},{},{}",
                farms.len(),
                total_resources.cru,
                total_resources.mru,
                total_resources.sru,
                total_resources.hru
            )
            .unwrap();
        }
    }
}

#[derive(Serialize)]
pub struct GraphQLRequest<'a, T: Serialize> {
    operation_name: &'a str,
    query: &'a str,
    variables: Option<T>,
}

#[derive(Deserialize)]
pub struct GraphQLResponse<T> {
    data: T,
}

#[derive(Deserialize)]
pub struct NodeReply {
    nodes: Vec<Node>,
}

#[derive(Deserialize)]
pub struct Node {
    #[serde(rename = "nodeID")]
    _node_id: u32,
    #[serde(rename = "farmID")]
    farm_id: u32,
    created: i64,
    #[serde(rename = "resourcesTotal")]
    resources_total: Resources,
}

#[derive(Deserialize)]
pub struct Resources {
    #[serde(deserialize_with = "de_u64")]
    cru: u64,
    #[serde(deserialize_with = "de_u64")]
    mru: u64,
    #[serde(deserialize_with = "de_u64")]
    sru: u64,
    #[serde(deserialize_with = "de_u64")]
    hru: u64,
}

/// Helper function to deserialize an u64 which is returned as string (BigNum) in graphql.
pub fn de_u64<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    Ok(match Value::deserialize(deserializer)? {
        Value::String(s) => s.parse().map_err(de::Error::custom)?,
        Value::Number(num) => num
            .as_u64()
            .ok_or_else(|| de::Error::custom("Invalid number"))?,
        _ => return Err(de::Error::custom("wrong type")),
    })
}
