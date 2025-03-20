use std::thread::sleep;
use std::time::{Duration};
use bytes::Bytes;
use tokio::time::Instant;
use ludis::db;


#[tokio::test]
async fn test_instant(){

    let now = Instant::now();
    println!("now: {:?}", now);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_db() {
    // let m = DashMap::new();
    // m.insert(String::from("test"), Bytes::from("test"));

    println!("test_db");
    let mut db_service = db::BucketDb::new(10);
    println!("{:?}", db_service.get(Bytes::from("aaacc")));
    db_service.set("aaa".into(), Bytes::from("aaaaadascasc"), Option::from(Duration::from_secs(2)));
    println!("{:?}", db_service.get("aaa".into()));
    // sleep(Duration::from_secs(5));
    tokio::time::sleep(Duration::from_secs(10)).await;
    println!("{:?}", db_service.get("aaa".into()));
}

#[tokio::test]
async fn test_new_keys(){
    let mut db_service = db::BucketDb::new(10);

    db_service.set_newest("aa".into(), Bytes::from("xzczxczx"), Duration::from_secs(10));
    db_service.set_newest("aa".into(), Bytes::from("aaaaadascasc"), Duration::from_secs(2));
    db_service.set_newest("aa".into(), Bytes::from("aaaaadascasc"), Duration::from_secs(2));
    db_service.set_newest("aa".into(), Bytes::from("aaaaadascasc"), Duration::from_secs(2));
    db_service.set_newest("aa".into(), Bytes::from("aaaaadascascxzczxc"), Duration::from_secs(3));
    println!("{:?}", db_service.get("aa".into()));

}





#[tokio::test]
async fn test_cmp(){
    let a = Bytes::from("aaa");
    let mut b = a.to_vec().clone();
    b.push(u8::MAX);
    let b = Bytes::from(b);
    println!("{:?}", a<b)
}



#[tokio::test]
async fn test_keys() {

    let mut db_service = db::BucketDb::new(10);
    db_service.set(Bytes::from("aa"), Bytes::from("aaaaadascasc"), Option::from(Duration::from_secs(2)));
    db_service.set(Bytes::from("vasdad"), Bytes::from("aaaaadascasc"), Option::from(Duration::from_secs(2)));
    db_service.set(Bytes::from("sadad"), Bytes::from("aaaaadascasc"), Option::from(Duration::from_secs(2)));
    db_service.set(Bytes::from("aqwewqe"), Bytes::from("aaaaadascasc"), Option::from(Duration::from_secs(2)));
    db_service.set(Bytes::from("vlzczxc"), Bytes::from("aaaaadascasc"), Option::from(Duration::from_secs(2)));
    db_service.set(Bytes::from("1313213213"), Bytes::from("aaaaadascasc"), Option::from(Duration::from_secs(2)));
    db_service.set(Bytes::from("asdasdsadasd"), Bytes::from("aaaaadascasc"), Option::from(Duration::from_secs(2)));
    db_service.set(Bytes::from("031dasdadawd"), Bytes::from("aaaaadascasc"), Option::from(Duration::from_secs(2)));
    println!("{:?}", db_service.keys(Some("a".into())));
    for i in db_service.iter(){
        println!("{:?}", i);
    }
}


#[tokio::test]
async fn test_timeout() {
    let mut db = db::BucketDb::new(10);
    db.set("aa".into(), Bytes::from("aaaaadascasc"), Option::from(Duration::from_secs(1)));
    db.set("aa".into(), Bytes::from("zxcxzc"), Option::from(Duration::from_secs(1)));
    db.set("aa".into(), Bytes::from("eweqw"), Option::from(Duration::from_secs(1)));
    db.set("aa".into(), Bytes::from("gfdgfdcz"), Option::from(Duration::from_secs(1)));
    tokio::time::sleep(Duration::from_secs(10)).await;
    println!("{:?}", db.get("aa".into()));;
}