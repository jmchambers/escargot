module Escargot
  class Railtie < ::Rails::Railtie
    initializer 'escargot.init' do
      
      ActiveSupport.on_load(:mongo_mapper) do
        ElasticSearch::Api::Hit.class_eval{include Escargot::HitExtensions}
        ElasticSearch::Client.class_eval{include Escargot::AdminIndexVersions}
        MongoMapper::Document.send :plugin, MongoMapperEsPlugin
        init_elastic_search_client
      end
      
      ActiveSupport.on_load(:active_record) do
        ElasticSearch::Api::Hit.class_eval{include Escargot::HitExtensions}
        ElasticSearch::Client.class_eval{include Escargot::AdminIndexVersions}
        include(Escargot::ActiveRecordExtensions)
        init_elastic_search_client
      end
    #ActiveSupport.on_load(:action_controller) do
    #  include(ElasticSearch::RequestLifecycle)
    #end
    end

    rake_tasks do
      load "tasks/escargot.tasks"
    end

    generators do
      load "generators/escargot_install/escargot_install_generator.rb"
    end

  end
end
