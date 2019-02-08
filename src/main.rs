// code based on mqtt-rs async example
extern crate mqtt;
#[macro_use]
extern crate log;
extern crate clap;
extern crate env_logger;
extern crate futures;
extern crate tokio;
extern crate uuid;
extern crate serde_json;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate slippy_map_tiles;
extern crate slippy_map_tilenames as smt;

use slippy_map_tiles::Tile;

use std::env;
use std::fmt::Debug;
use std::io::Write;
use std::net;
use std::str;
use std::time::{Duration, Instant};

use clap::{App, Arg};

use uuid::Uuid;

use futures::{future, Future, Stream};

use tokio::io::{self, AsyncRead};
use tokio::net::TcpStream;
use tokio::timer::Interval;

use mqtt::control::variable_header::ConnectReturnCode;
use mqtt::packet::*;
use mqtt::TopicFilter;
use mqtt::{Decodable, Encodable, QualityOfService};

use std::cmp::Ordering;

use std::error;
use std::fmt;

type Result<T> = std::result::Result<T, CoordinateError>;
#[derive(Debug, Clone)]
struct CoordinateError {
    name: String
}
impl CoordinateError {
    fn raise(coordinate: String) -> CoordinateError {
        CoordinateError { name: coordinate }
    }
}
impl fmt::Display for CoordinateError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "'{}' is empty", self.name)
    }
}
// This is important for other errors to wrap this one.
impl error::Error for CoordinateError {
    fn description(&self) -> &str {
        "Coordinate was empty"
    }

    fn cause(&self) -> Option<&error::Error> {
        // Generic error, underlying cause isn't tracked.
        None
    }
}

fn generate_client_id() -> String {
    format!("/MQTT/rust/{}", Uuid::new_v4())
}

fn alt_drop<E: Debug>(err: E) {
    warn!("{:?}", err);
}

// which ZOOM level to use for tiles
const ZOOM: u8 = 19;
// tile server baseurl
const TILE_SERVER_URL: &str = "https://a.tile.openstreetmap.org";

// struct to assist serde_json in deserialization.
// https://digitransit.fi/en/developers/apis/4-realtime-api/vehicle-positions/
#[derive(Serialize, Deserialize, Debug)]
struct VP {
    desi: Option<String>,
    dir: Option<String>,
    oper: Option<u32>,
    veh: Option<u32>,
    tst: Option<String>,
    tsi: Option<u32>,
    spd: Option<f32>,
    hdg: Option<u32>,
    lat: Option<f32>,
    long: Option<f32>,
    acc: Option<f32>,
    dl: Option<i32>,
    odo: Option<u32>,
    drst: Option<u32>,
    oday: Option<String>,
    jrn: Option<u32>,
    line: Option<u32>,
    start: Option<String>
}
#[allow(non_snake_case)]
#[derive(Serialize, Deserialize, Debug)]
struct Payload {
    VP: VP
}
impl Payload {
    fn parse(msg: &str) -> Payload {
        serde_json::from_str(msg).unwrap()
    }

    fn slippy_tilename_url(&self) -> Result<String> {
        let long: f32 = self.VP.long.ok_or(CoordinateError::raise("longitude".to_string())).unwrap();
        let lat: f32 = self.VP.lat.ok_or(CoordinateError::raise("latitude".to_string())).unwrap();

        let l2t = smt::lonlat2tile(long.into(), lat.into(), ZOOM);
        let tile = Tile::new(ZOOM, l2t.0, l2t.1).unwrap();
        Ok(format!("{}/{}.png", TILE_SERVER_URL, tile.zxy()))
    }

    fn get_desi(&self) -> String {
        self.VP.desi.as_ref().unwrap().clone()
    }

    fn get_veh(&self) -> u32 {
        self.VP.veh.as_ref().unwrap().clone()
    }

    fn get_dl(&self) -> i32 {
        self.VP.dl.as_ref().unwrap().clone()
    }
}

fn process_payload(payload: &Payload)
{
    let desi: String = payload.get_desi();
    let veh: u32 = payload.get_veh();
    let dl: i32 = payload.get_dl();

    match dl.cmp(&0) {
        Ordering::Equal => {
            info!("Line {} ({}) is on schedule.", desi, veh);
        },
        Ordering::Less => {
            warn!("Line {} ({}) is currently behind schedule by {} seconds.", desi, veh, dl);
        },
        Ordering::Greater => {
            warn!("Line {} ({}) is currently ahead of schedule by {} seconds.", desi, veh, dl);
        }
    };

    match payload.slippy_tilename_url() {
        Ok(url) => info!(" {}", url),
        Err(err) => error!(" {}", err)
    };

}

fn main() {
    // configure logging
    env::set_var("RUST_LOG", env::var_os("RUST_LOG").unwrap_or_else(|| "info".into()));
    env_logger::init();

    let matches = App::new("digitransit-rt")
        .author("Antti Peltonen <bcow@iki.fi>")
        .arg(
            Arg::with_name("SUBSCRIBE")
                .short("s")
                .long("subscribe")
                .takes_value(true)
                .multiple(true)
                .required(true)
                .default_value("/hfp/v1/journey/ongoing/+/+/+/+/+/+/+/+/0/#")
                .help("Channel filter to subscribe. Example: \"/hfp/v1/journey/ongoing/train/#\""),
        ).arg(
            Arg::with_name("CLIENT_ID")
                .short("i")
                .long("client-identifier")
                .takes_value(true)
                .help("Client identifier"),
        ).get_matches();

    let server_addr = "mqtt.hsl.fi:1883";
    let client_id = matches
        .value_of("CLIENT_ID")
        .map(|x| x.to_owned())
        .unwrap_or_else(generate_client_id);
    let channel_filters: Vec<(TopicFilter, QualityOfService)> = matches
        .values_of("SUBSCRIBE")
        .unwrap()
        .map(|c| (TopicFilter::new(c.to_string()).unwrap(), QualityOfService::Level0))
        .collect();

    let keep_alive = 10;

    info!("Connecting to {:?} ... ", server_addr);
    let mut stream = net::TcpStream::connect(server_addr).unwrap();
    info!("Connected!");

    debug!("Client identifier {:?}", client_id);
    let mut conn = ConnectPacket::new("MQTT", client_id);
    conn.set_clean_session(true);
    conn.set_keep_alive(keep_alive);
    let mut buf = Vec::new();
    conn.encode(&mut buf).unwrap();
    stream.write_all(&buf[..]).unwrap();

    let connack = ConnackPacket::decode(&mut stream).unwrap();
    trace!("CONNACK {:?}", connack);

    if connack.connect_return_code() != ConnectReturnCode::ConnectionAccepted {
        panic!(
            "Failed to connect to server, return code {:?}",
            connack.connect_return_code()
        );
    }

    // const CHANNEL_FILTER: &'static str = "typing-speed-test.aoeu.eu";
    info!("Applying channel filters {:?} ...", channel_filters);
    let sub = SubscribePacket::new(10, channel_filters);
    let mut buf = Vec::new();
    sub.encode(&mut buf).unwrap();
    stream.write_all(&buf[..]).unwrap();

    loop {
        let packet = match VariablePacket::decode(&mut stream) {
            Ok(pk) => pk,
            Err(err) => {
                error!("Error in receiving packet {:?}", err);
                continue;
            }
        };
        trace!("PACKET {:?}", packet);

        if let VariablePacket::SubackPacket(ref ack) = packet {
            if ack.packet_identifier() != 10 {
                panic!("SUBACK packet identifier not match");
            }

            info!("Subscribed!");
            break;
        }
    }

    // connection made, start the async work
    let program = future::ok(()).and_then(move |()| {
        let stream = TcpStream::from_std(stream, &Default::default()).unwrap();
        let (mqtt_read, mqtt_write) = stream.split();

        let ping_time = Duration::new((keep_alive / 2) as u64, 0);
        let ping_stream = Interval::new(Instant::now() + ping_time, ping_time);

        let ping_sender = ping_stream.map_err(alt_drop).fold(mqtt_write, |mqtt_write, _| {
            trace!("Sending PINGREQ to broker");

            let pingreq_packet = PingreqPacket::new();

            let mut buf = Vec::new();
            pingreq_packet.encode(&mut buf).unwrap();
            io::write_all(mqtt_write, buf)
                .map(|(mqtt_write, _buf)| mqtt_write)
                .map_err(alt_drop)
        });

        let receiver = future::loop_fn::<_, (), _, _>(mqtt_read, |mqtt_read| {
            VariablePacket::parse(mqtt_read).map(|(mqtt_read, packet)| {
                trace!("PACKET {:?}", packet);

                match packet {
                    VariablePacket::PingrespPacket(..) => {
                        trace!("Receiving PINGRESP from broker ..");
                    }
                    VariablePacket::PublishPacket(ref publ) => {
                        let msg = match str::from_utf8(&publ.payload_ref()[..]) {
                            Ok(msg) => msg,
                            Err(err) => {
                                error!("Failed to decode publish message {:?}", err);
                                return future::Loop::Continue(mqtt_read);
                            }
                        };
                        debug!("PUBLISH ({}): {}", publ.topic_name(), msg);
                        let payload = Payload::parse(msg);

                        process_payload(&payload);
                    }
                    _ => {}
                }

                future::Loop::Continue(mqtt_read)
            })
        }).map_err(alt_drop);

        ping_sender.join(receiver).map(alt_drop)
    });

    tokio::run(program);
}
