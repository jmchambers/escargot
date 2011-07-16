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
      
      start = options.delete(:start) || 0
      batch_size = options.delete(:batch_size) || 1000
      
      begin
        records = self.where.limit(batch_size).skip(start).to_a
        yield records unless records.empty?
        start += batch_size
      end while records.size == batch_size
      
    end

  end

  module InstanceMethods

  end
end