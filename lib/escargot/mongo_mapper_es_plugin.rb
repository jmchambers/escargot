module MongoMapperEsPlugin
  extend ActiveSupport::Concern

  included do
    
    include Escargot::ActiveRecordExtensions

  end

  module ClassMethods
    
    def table_exists?
      true
    end
    
    def find_in_batches(options = {})
      
      query      = options[:query]      || {}
      start      = options[:start]      || 0
      batch_size = options[:batch_size] || 1000
      fields     = *options[:fields]
      
      begin
        records = self.where(query).limit(batch_size).skip(start).fields(fields).to_a
        yield records unless records.empty?
        start += batch_size
      end while records.size == batch_size
      
    end

  end

end