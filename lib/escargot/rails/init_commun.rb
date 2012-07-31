def init_elastic_search_client
	path_to_elasticsearch_config_file = Rails.root.to_s + "/config/escargot.yml"

	unless File.exists?(path_to_elasticsearch_config_file)
	  Rails.logger.warn "No config/escargot.yaml file found, connecting to localhost:9200"
	  Escargot.client = ElasticSearch.new("http://localhost:9200") do |faraday|
	    faraday.adapter :patron
	  end
	else 
	  config = YAML.load_file(path_to_elasticsearch_config_file)
	  Escargot.client = ElasticSearch.new(config["host"] + ":" + config["port"].to_s, :timeout => 20) do |faraday|
	    faraday.adapter :patron
	  end
	end
end 
