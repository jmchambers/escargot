require 'will_paginate/collection'

module Escargot
  module ActiveRecordExtensions

    extend ActiveSupport::Concern
  
    included do
      
    end

    module ClassMethods
      attr_accessor :index_name
      attr_accessor :update_index_policy
      attr_accessor :mapping
      attr_accessor :index_options
      attr_accessor :indexing_options

      # defines an elastic search index. Valid options:
      #
      # :index_name (will default class name using method "underscore")
      #
      # :updates, how to to update the contents of the index when a document is changed, valid options are:
      #
      #   - false: do not update the index
      #
      #   - :immediate: update the index but do not refresh it automatically.
      #     With the default settings, this means that the change may take up to 1 second
      #     to be seen by other users.
      #
      #     See: http://www.elasticsearch.com/docs/elasticsearch/index_modules/engine/robin/
      #
      #     This is the default option.
      #
      #   - :immediate_with_refresh: update the index AND ask elasticsearch to refresh it after each
      #     change. This garantuees that the changes will be seen by other users, but may affect
      #     performance.
      #
      #   - :enqueu: enqueue the document id so that a remote worker will update the index
      #     This is the recommended options if you have set up a job queue (such as Resque)
      #

      def elastic_index(options = {})
        
        options.symbolize_keys!
        Escargot.register_model(self)
        
        if respond_to?('single_collection_inherited?') and single_collection_inherited?
          ivars = %w[@index_name @update_index_policy @index_options @indexing_options @mapping]
          ivars.each do |ivar|
            if passed_option = options[ivar[1..-1].to_sym]
              instance_variable_set ivar, passed_option
            else
              instance_variable_set ivar, superclass.instance_variable_get(ivar)
            end
          end
        else
          @index_name = options[:index_name] || self.name.underscore.gsub(/\//,'-')
          @update_index_policy = options.include?(:updates) ? options[:updates] : :immediate
          
          if @update_index_policy
            after_save :update_index
            after_destroy :delete_from_index
          end
          
          @index_options    = options[:index_options]    || {}
          @indexing_options = options[:indexing_options] || {}
          @mapping          = options[:mapping]          || false
        end
        
      end

      def search(query, options={})
        Escargot.search(query, options.reverse_merge!(:index => self.index_name, :type => elastic_search_type), true)
      end
      
      def search_hits(query, options = {})
        Escargot.search_hits(query, options.reverse_merge!(:index => self.index_name, :type => elastic_search_type), true)
      end  

      def search_count(query = "*", options = {})
        Escargot.search_count(query, options.reverse_merge!(:index => self.index_name, :type => elastic_search_type), true)
      end

      def facets(fields_list, options = {})
        size = options.delete(:size) || 10
        fields_list = [fields_list] unless fields_list.kind_of?(Array)
        
        if !options[:query]
          options[:query] = {:match_all => { } }
        elsif options[:query].kind_of?(String)
          options[:query] = {:query_string => {:query => options[:query]}}
        end

        options[:facets] = {}
        fields_list.each do |field|
          options[:facets][field] = {:terms => {:field => field, :size => size}}
        end

        hits = Escargot.client.search(options, {:index => self.index_name, :type => elastic_search_type})
        out = {}
        
        fields_list.each do |field|
          out[field.to_sym] = {}
          hits.facets[field.to_s]["terms"].each do |term|
            out[field.to_sym][term["term"]] = term["count"]
          end
        end

        out
      end

      # explicitly refresh the index, making all operations performed since the last refresh
      # available for search
      #
      # http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/indices/refresh/
      def refresh_index(index_version = nil)
        Escargot.client.refresh(index_version || index_name)
      end
      
      # creates a new index version for this model and sets the mapping options for the type
      def create_index_version
        index_version = Escargot.client.create_index_version(@index_name, @index_options)
        if @mapping
          update_mapping(index_version)
        end
        index_version
      end
      
      def update_mapping(index_version = nil)
        index_version ||= Escargot.client.current_index_version(index_name)
        Escargot.client.update_mapping(@mapping, :index => index_version, :type => elastic_search_type)
      end
      
      # deletes all index versions for this model and the alias (if exist)
      def delete_index
        # set current version to delete alias later
        current_version = Escargot.client.current_index_version(index_name)

        # deletes any index version and the alias
        Escargot.client.index_versions(index_name).each{|index_version|
          Escargot.client.alias_index(:remove => {index_version => index_name}) if (index_version == current_version)
          Escargot.client.delete_index(index_version)
        }

        # and delete the index itself if it exists
        begin
          Escargot.client.delete_index(index_name)
        rescue ElasticSearch::RequestError
          # it's ok, this means that the index doesn't exist
        end
      end
      
      def delete_id_from_index(id, options = {})
        options[:index] ||= self.index_name
        options[:type]  ||= elastic_search_type
        Escargot.client.delete(id.to_s, options)
      end
      
      def optimize_index
        Escargot.client.optimize(index_name)
      end
      
      def elastic_search_type
        self.name.underscore.singularize.gsub(/\//,'-')
      end

    end

    # updates the index using the appropiate policy
    def update_index
      if self.class.update_index_policy == :immediate_with_refresh
        local_index_in_elastic_search(:refresh => true)
      elsif self.class.update_index_policy == :enqueue
        Resque.enqueue(DistributedIndexing::ReIndexDocuments, self.class.to_s, [self.id])
      else
        local_index_in_elastic_search
      end
    end

    # deletes the document from the index using the appropiate policy ("simple" or "distributed")
    def delete_from_index
      if self.class.update_index_policy == :immediate_with_refresh
        self.class.delete_id_from_index(self.id, :refresh => true)
        # As of Oct 25 2010, :refresh => true is not working
        self.class.refresh_index()
      elsif self.class.update_index_policy == :enqueue
        Resque.enqueue(DistributedIndexing::ReIndexDocuments, self.class.to_s, [self.id])
      else
        self.class.delete_id_from_index(self.id)
      end
    end

    def local_index_in_elastic_search(options = {})
      
      default_options = {
        :index => self.class.index_name,
        :type  => self.class.elastic_search_type,
        :id    => self.id.to_s,
      }
      
      options.reverse_merge! default_options
      
      if options[:indexing_options]
        indexing_options = options.delete(:indexing_options)
      else
        indexing_options = self.respond_to?(:indexing_options) ? self.indexing_options : {}
      end
      
      options.merge! indexing_options
      
      unless doc = options.delete(:doc)
        doc = self.respond_to?(:indexed_attributes) ? self.indexed_attributes : self.attributes
      end
      
      #bulk-ready client passed?
      if options.has_key?(:bulk_client)
        bulk_loading = true
        client = options.delete(:bulk_client)
      else
        bulk_loading = false
        #doc = doc.to_json
        client = Escargot.client
      end

      client.index(doc, options)

      ## !!!!! passing :refresh => true should make ES auto-refresh only the affected
      ## shards but as of Oct 25 2010 with ES 0.12 && rubberband 0.0.2 that's not the case
      if options[:refresh] and not bulk_loading
        self.class.refresh_index(options[:index])
      end
        
    end

  end
end