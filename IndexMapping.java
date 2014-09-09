Client client = new TransportClient()
	.addTransportAddress(new InetSocketTransportAddress("HOST1", 9300))
	.addTransportAddress(new InetSocketTransportAddress("HOST2", 9300));
	
//Client client = NodeBuilder.nodeBuilder().client(true).node().client();
client.admin().indices().prepareCreate(INDEX).execute().actionGet();
XContentBuilder builder = XContentFactory.jsonBuilder().
	startObject().
		startObject(DOCUMENT_TYPE).
			startObject("properties").
				startObject("path").
					field("type", "string").field("store", "yes").field("index", "not_analyzed").
				endObject().
				startObject("title").
					field("type", "string").field("store", "yes").field("analyzer", "german").
				endObject().
				// more mapping
			endObject().
		endObject().
	endObject();
client.admin().indices().preparePutMapping(index).setType(DOCUMENT_TYPE).setSource(builder).execute().actionGet();
client.close();
