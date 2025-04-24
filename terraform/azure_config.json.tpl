{
    "tables": {
        "labels": {
            "url": "https://${query_server_labels_fqdn}",
            "port": 8001,
            "table_name": "labels",
            "filename": "discogs_20250401_labels.csv.gz"
        },
        "artists": {
            "url": "https://${query_server_artists_fqdn}",
            "port": 8002,
            "table_name": "artists",
            "filename": "discogs_20250401_artists.csv.gz"
        }
    }
}
