use tytodb_client::{alba, client_thread, handler::{BatchBuilder, CreateContainerBuilder, CreateRowBuilder, DeleteContainerBuilder, SearchBuilder}, lo, logical_operators::LogicalOperator, ToAlbaAlbaTypes, BIGINT, MEDIUM_STRING};
use std::{fs::File, os::unix::fs::FileExt};

#[test]
fn test_tytodb_routine() {
    // NOTE: For this test to run, you need a 'secret_key_path' file in the project root
    // containing a 32-byte secret key, and a TytoDB instance running at 127.0.0.1:4287.
    let mut secret = [0u8;32];
    eprintln!("--> reading the secret file");
    if let Ok(f) = File::open("secret_key_path"){
        f.read_exact_at(&mut secret,0).unwrap();
    } else {
        panic!("secret_key_path not found. Please create it with a 32-byte secret key.");
    }
    eprintln!("
==> secret file read succesfully");
    
    eprintln!("--> connecting to tytodb");
    let client = client_thread::Client::connect("127.0.0.1:4287", secret).expect("Failed to connect to TytoDB");
    eprintln!("
==> connected to tytodb succesfully");   
    
    let create_container_builder = CreateContainerBuilder::new()
    .put_container("nice_container".to_string())
    .insert_header("id".to_string(), BIGINT)
    .insert_header("content".to_string(), MEDIUM_STRING);
    
    eprintln!("--> Creating container 'nice_container'");
    client.execute(create_container_builder.finish().unwrap()).expect("Failed to create container");
    eprintln!("==> Container 'nice_container' created succesfully");

    for w in 1..10{ // Reduced loop count for quicker test execution
        let mut batched = BatchBuilder::new();
        batched = batched.transaction(true);
        
        let create_main_row = CreateRowBuilder::new()
        .put_container("nice_container".to_string())
        .insert_value("id".to_string(), alba!(w))
        .insert_value("content".to_string(), alba!("legal-legal-legal".to_string()));

        batched = batched.push(create_main_row);

        eprintln!("
~~> Batching multiple requests (iteration {})", w);
        client.execute(batched.finish().unwrap()).expect("Failed to execute batch");
        eprintln!("
--> Batching multiple requests finished
");

        let search_main_row = SearchBuilder::new()
            .add_container("nice_container".to_string())
            .add_column_name("id".to_string())
            .add_conditions( ( "id".to_string(), lo!(=), alba!(w) ) , true)
            .add_conditions( ( "content".to_string(), lo!(!=), alba!("paia-paia".to_string()) ), true)
            .add_conditions( ( "content".to_string(), lo!("&>"), alba!("legal-legal-legal".to_string()) ), true)
            .add_conditions( ( "content".to_string(), lo!("&&>"), alba!("legal-legal-legal".to_string()) ), true)
            .add_conditions( ( "content".to_string(), lo!(regex), alba!("legal-legal-legal".to_string()) ), true)
            .add_conditions( ( "id".to_string() , lo!(>), alba!(0) ), true )
            .add_conditions( ( "id".to_string() , lo!(<), alba!(w+2) ), true )
            .add_conditions( ( "id".to_string() , lo!(>=), alba!(w) ), true )
            .add_conditions( ( "id".to_string() , lo!(<=), alba!(w) ), true );

        eprintln!("--> Searching for row with id {}", w);
        let list = client.execute(search_main_row.finish().unwrap()).expect("Search failed").row_list;
        eprintln!("==> Search finished without errors");
        eprintln!("=== ROW-LIST-LENGTH: {}",list.len());
        eprintln!("
=== LIST: {:?}",list);
        assert_eq!(list.len(), 1, "Expected exactly one row for id {}", w);
        assert_eq!(list[0].get("id").unwrap().as_bigint().unwrap(), w as i64, "ID mismatch for row {}", w);
    }
    
    eprintln!("--> Deleting container 'nice_container'");
    let delc = DeleteContainerBuilder::new().put_container("nice_container".to_string());
    client.execute(delc.finish().unwrap()).expect("Failed to delete container");
    eprintln!("==> Container 'nice_container' deleted succesfully");
}
