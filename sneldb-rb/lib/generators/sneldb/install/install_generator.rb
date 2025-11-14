require "rails/generators"

module SnelDB
  module Generators
    class InstallGenerator < Rails::Generators::Base
      source_root File.expand_path("templates", __dir__)

      desc "Creates a SnelDB initializer file"

      def create_initializer
        template "sneldb.rb", "config/initializers/sneldb.rb"
      end
    end
  end
end

