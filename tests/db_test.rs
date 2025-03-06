use std::time::{Duration};
use bytes::Bytes;
use tokio::time::Instant;
use ludis::db;


#[tokio::test]
async fn test_instant(){

    let now = Instant::now();
    println!("now: {:?}", now);
}

#[tokio::test]
async fn test_db() {
    // let m = DashMap::new();
    // m.insert(String::from("test"), Bytes::from("test"));

    println!("test_db");
    let mut db_service = db::BucketDb::new(10);
    println!("{:?}", db_service.get("aaacc".to_string()));
    db_service.set("aaa".to_string(), Bytes::from("aaaaadascasc"), Option::from(Duration::from_secs(2)));
    println!("{:?}", db_service.get("aaa".to_string()));
    tokio::time::sleep(Duration::from_secs(10)).await;
    println!("{:?}", db_service.get("aaa".to_string()));
}

#[tokio::test]
async fn test_new_keys(){
    let mut db_service = db::BucketDb::new(10);

    db_service.set_newest("aa".to_string(), Bytes::from("xzczxczx"), Instant::now() + Duration::from_secs(10));
    db_service.set_newest("aa".to_string(), Bytes::from("aaaaadascasc"), Instant::now() + Duration::from_secs(2));
    db_service.set_newest("aa".to_string(), Bytes::from("aaaaadascasc"), Instant::now() + Duration::from_secs(2));
    db_service.set_newest("aa".to_string(), Bytes::from("aaaaadascasc"), Instant::now() + Duration::from_secs(2));
    db_service.set_newest("aa".to_string(), Bytes::from("aaaaadascascxzczxc"), Instant::now() + Duration::from_secs(3));
    println!("{:?}", db_service.get("aa".to_string()));
}





#[tokio::test]
async fn test_keys() {
    let mut db_service = db::BucketDb::new(10);

    db_service.set("aa".to_string(), Bytes::from("aaaaadascasc"), Option::from(Duration::from_secs(2)));
    db_service.set("vasdad".to_string(), Bytes::from("aaaaadascasc"), Option::from(Duration::from_secs(2)));
    db_service.set("sadad".to_string(), Bytes::from("aaaaadascasc"), Option::from(Duration::from_secs(2)));
    db_service.set("aqwewqe".to_string(), Bytes::from("aaaaadascasc"), Option::from(Duration::from_secs(2)));
    db_service.set("vlzczxc".to_string(), Bytes::from("aaaaadascasc"), Option::from(Duration::from_secs(2)));
    db_service.set("1313213213".to_string(), Bytes::from("aaaaadascasc"), Option::from(Duration::from_secs(2)));
    db_service.set("asdasdsadasd".to_string(), Bytes::from("aaaaadascasc"), Option::from(Duration::from_secs(2)));
    db_service.set("031dasdadawd".to_string(), Bytes::from("aaaaadascasc"), Option::from(Duration::from_secs(2)));
    println!("{:?}", db_service.keys(Some("0".to_string())));
}

#[tokio::test]
async fn test_subscribe() {
    let mut db = db::BucketDb::new(10);

    let mut re = db.subscribe("aaaa".to_string());
    let mut re2 = db.subscribe("aaaa".to_string());
    let pe = db.publish("aaaa", Bytes::from("aaaaadascasc"));
    println!("{:?}", re.recv().await.unwrap());
    // println!("{:?}", re.recv().await.unwrap());
    println!("{:?}", re2.recv().await.unwrap());

}



#[tokio::test]
async fn test_timeout() {
    let mut db = db::BucketDb::new(10);
    db.set(String::from("aa"), Bytes::from("aaaaadascasc"), Option::from(Duration::from_secs(10)));
    db.set(String::from("aa"), Bytes::from("zxcxzc"), Option::from(Duration::from_secs(10)));
    db.set(String::from("aa"), Bytes::from("eweqw"), Option::from(Duration::from_secs(10)));
    db.set(String::from("aa"), Bytes::from("gfdgfd"), Option::from(Duration::from_secs(10)));
    println!("{:?}", db.get("aa".to_string()));;
}