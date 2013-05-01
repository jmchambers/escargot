require 'will_paginate/collection'

module Escargot
  module ActiveRecordExtensions

    extend ActiveSupport::Concern
  
    included do
      SCHEMA_CHECK_INTERVAL ||= 5.minutes
    end

    module ClassMethods
      attr_accessor :index_name
      attr_accessor :update_index_policy
      attr_accessor :mapping
      attr_accessor :index_options
      attr_accessor :indexing_options
      
      attr_accessor :current_schema_version
      attr_accessor :previous_schema_version

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
          ivars = %w[@index_name @update_index_policy @index_options @indexing_options @mapping @current_schema_version @previous_schema_version]
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
          @current_schema_version  = options[:current_schema_version]  || "0"
          @previous_schema_version = options[:previous_schema_version] || "0"
        end
        
      end

      def all_index_versions
        Escargot.client.index_versions(index_name)
      end

      def current_index_name
        raise "missing index name" unless index_name
        "current_#{index_name}"
      end
      
      def previous_index_name
        raise "missing index name" unless index_name
        "previous_#{index_name}"
      end
      
      def index_with_target_schema_ready?
        #TODO MAKE SURE THIS REPORTS true/false CORRECTLY THROUGHOUT INDEX MIGRATION
        #  TRUE when the new index is in place, and this app can pass properly structured queries to it
        #  FALSE when the new index isn't ready
        if @schema_checked_at.nil? or Time.now - @schema_checked_at > self::SCHEMA_CHECK_INTERVAL
          @schema_checked_at = Time.now
          @current_schema_is_target = current_index_schema_version == @current_schema_version
        else
          @current_schema_is_target
        end
      end
      
      def current_index_schema_version
        Escargot.client.current_index_schema_version(current_index_name)
      end
      
      def current_index_version
        Escargot.client.current_index_version(current_index_name)
      end
      
      def previous_index_version
        Escargot.client.current_index_version(previous_index_name)
      end
      
      def current_search_index_name
        #TODO make searches use this rather than index_name
        if index_with_target_schema_ready?
          current_index_name
        else
          previous_index_name
        end
      end
  
      def prune_old_indices
        Escargot.client.prune_index_versions(index_name)
      end

      def search(query, options={})
        Escargot.search(query, options.reverse_merge!(:index => current_search_index_name, :type => elastic_search_type), true)
      end
      
      def search_hits(query, options = {})
        Escargot.search_hits(query, options.reverse_merge!(:index => current_search_index_name, :type => elastic_search_type), true)
      end  

      def search_count(query = "*", options = {})
        Escargot.search_count(query, options.reverse_merge!(:index => current_search_index_name, :type => elastic_search_type), true)
      end

      # def facets(fields_list, options = {})
        # size = options.delete(:size) || 10
        # fields_list = [fields_list] unless fields_list.kind_of?(Array)
#         
        # if !options[:query]
          # options[:query] = {:match_all => { } }
        # elsif options[:query].kind_of?(String)
          # options[:query] = {:query_string => {:query => options[:query]}}
        # end
# 
        # options[:facets] = {}
        # fields_list.each do |field|
          # options[:facets][field] = {:terms => {:field => field, :size => size}}
        # end
# 
        # hits = Escargot.client.search(options, {:index => current_search_index_name, :type => elastic_search_type})
        # out = {}
#         
        # fields_list.each do |field|
          # out[field.to_sym] = {}
          # hits.facets[field.to_s]["terms"].each do |term|
            # out[field.to_sym][term["term"]] = term["count"]
          # end
        # end
# 
        # out
      # end

      # explicitly refresh the index, making all operations performed since the last refresh
      # available for search
      #
      # http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/indices/refresh/
      def refresh_index(index_version = nil)
        Escargot.client.refresh(index_version || current_search_index_name)
      end
      
      # creates a new index version for this model and sets the mapping options for the type
      def create_index_version
        index_version  = Escargot.client.create_index_version(@index_name, @index_options, @current_schema_version)
        if @mapping
          update_mapping(index_version)
        end
        index_version
      end
      
      def update_mapping(index_version = nil)
        index_version ||= current_index_version
        Escargot.client.update_mapping(@mapping, :index => index_version, :type => elastic_search_type) if @mapping.present?
      end
      
      # deletes all index versions for this model and the alias (if exist)
      def delete_index
        # set current version to delete alias later
        current_version = current_index_version

        # deletes any index version and the alias
        all_index_versions.each{|index_version|
          Escargot.client.alias_index(:remove => {index_version => current_index_name}) if (index_version == current_version)
          Escargot.client.delete_index(index_version)
        }

        # and delete the index itself if it exists
        begin
          Escargot.client.delete_index(current_index_name)
        rescue ElasticSearch::RequestError
          # it's ok, this means that the index doesn't exist
        end
      end
      
      def delete_id_from_index(id, options = {})
        options[:index] ||= current_search_index_name
        options[:type]  ||= elastic_search_type
        Escargot.client.delete(id.to_s, options)
      end
      
      def optimize_index
        Escargot.client.optimize(current_index_name)
      end
      
      def elastic_search_type
        self.name.underscore.singularize.gsub(/\//,'-')
      end

    end

    # updates the index using the appropiate policy
    def update_index(policy = nil)
      policy ||= self.class.update_index_policy
      case policy
      when :immediate_with_refresh
        local_index_in_elastic_search(:refresh => true)
      when :enqueue
        DistributedIndexing::ReIndexDocuments.perform_in(1.seconds, self.class.to_s, [self.id.to_s])
      else
        local_index_in_elastic_search
      end
    end

    # deletes the document from the index using the appropiate policy ("simple" or "distributed")
    def delete_from_index
      case self.class.update_index_policy
      when :immediate_with_refresh
        self.class.delete_id_from_index(self.id, :refresh => true)
        # As of Oct 25 2010, :refresh => true is not working
        self.class.refresh_index()
      when :enqueue
        DistributedIndexing::ReIndexDocuments.perform_async(self.class.to_s, [self.id.to_s])
      else
        self.class.delete_id_from_index(self.id)
      end
    end

    def local_index_in_elastic_search(options = {})
      
      default_options = {
        :type  => self.class.elastic_search_type,
        :id    => self.id.to_s,
      }
      
      #we create clone here to avoid side-effects
      options = options.reverse_merge default_options
      
      indexing_options = options.delete(:indexing_options) || ( respond_to?(:indexing_options) ? indexing_options : {} )
      options.merge! indexing_options
      
      #bulk-ready client passed?
      if options.has_key?(:bulk_client)
        bulk_loading = true
        client = options.delete(:bulk_client)
      else
        bulk_loading = false
        client = Escargot.client
      end
      
      
      doc = options.delete(:doc) || ( respond_to?(:indexed_attributes) ? indexed_attributes : attributes )
      if options[:index]
        #if an index is specified we only index to that one
        # and we ALWAYS use the latest #indexed_attributes method as we only ever specify the index to write to
        # when we are building a new index
        client.index(doc, options)
        
      else
        #we need to do different things depending on whether
        # a) we are midway through building a new index (indicated by there being more than one)
        # b) we are changing from one schemea to another (indicated by the provision of two different styles of doc)
        indices        = self.class.all_index_versions.first(2)
        have_old_style = options[:old_style_doc] || respond_to?(:old_style_indexed_attributes)
        
        docs = indices.map do |index_version|
          
          schema = Escargot.client.extract_schema_version_from_index(index_version)
          case schema
          when self.class.current_schema_version
            :doc
          when self.class.previous_schema_version
            :old_style_doc if have_old_style
          end
          
        end
        
        #only build old_style_doc if we actually need it
        if docs.include?(:old_style_doc)
          old_style_doc = options.delete(:old_style_doc) || old_style_indexed_attributes
        end
        
        indices.each_with_index do |index_version, i|
          options[:index] = index_version
          curr_doc = case docs[i]
          when :doc
            client.index(doc, options)
          when :old_style_doc
            client.index(old_style_doc, options)
          end
        end
        
      end
      
      ## !!!!! passing :refresh => true should make ES auto-refresh only the affected
      ## shards but as of Oct 25 2010 with ES 0.12 && rubberband 0.0.2 that's not the case
      if options[:refresh] and not bulk_loading
        self.class.refresh_index(options[:index])
      end
        
    end

  end
end


        
#         
        # can_handle_current  = schemas.first == @current_schema_version
        # can_handle_previous = schemas.last  == @previous_schema_version
#         
        # unless can_handle_current or can_handle_previous
          # schemas_in_codebase = [@current_schema_version, @previous_schema_version]
          # raise "can't index:\n#{self}\nbecause this codebase covers schemas #{schemas_in_codebase}, while the latest indices in ES are #{schemas}"
        # end
#         
        # new_index_ready = respond_to?(:index_with_target_schema_ready?) ? index_with_target_schema_ready? : nil
        # have_old_style  = options[:old_style_doc] || respond_to?(:old_style_indexed_attributes)
#         
        # if can_handle_previous and indices.length == 1
#           
          # if have_new_style and not can_handle_previous
            # #we have the code for the old_style_doc, but we only have one index
            # # because index_with_target_schema_ready? == true, we assume the migration is over and so we use the lastest doc style
            # docs = [:doc]
#             
          # else
            # #we have loaded the new app, but we haven't started to build the new index yet
            # #so we carry on using the old_style_doc
            # docs = [:old_style_doc]
#             
          # end
#           
        # elsif have_old_style and can_handle_previous and indices.length == 2
          # #we must be mid-migration as there are two indices, so we send the new and old styles to their respective indices
          # docs = [:doc, :old_style_doc]
#           
        # else
          # #if we don't have an old_style_doc, but we do have 2 indices then we must be building an index with the same schema
          # # or a schema change that won't break existing search code
          # # or perhaps just a mapping change in ES
#           
          # #if we only have 1 index, and no old_style_doc, then we're not in the middle of an index build
# 
          # #either way, we just send the same doc to however many indices there are
          # docs = [:doc, :doc]
# 
        # end