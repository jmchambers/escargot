module Escargot

  module LocalIndexing
    
    def LocalIndexing.create_index_for_model(model, options = {})
      model = model.constantize if model.kind_of?(String)

      index_version = model.create_index_version

      if $elastic_search_client.respond_to?(:bulk)
        bulk_index(model, index_version, options)
      else
        incremental_index(model, index_version)
      end

      $elastic_search_client.deploy_index_version(model.index_name, index_version)
    end

    def LocalIndexing.incremental_index(model, index_version)
      model.find_in_batches do |batch|
        batch.each do |record|
          record.local_index_in_elastic_search(:index => index_version)
        end
      end
    end

    def LocalIndexing.bulk_index(model, index_version, options = {})
      model.find_in_batches(options) do |batch|
        $elastic_search_client.bulk do |bulk_client|
          batch.each do |record|
            record.local_index_in_elastic_search(:index => index_version, :bulk_client => bulk_client)
          end
        end
      end
    end
    
  end

end
