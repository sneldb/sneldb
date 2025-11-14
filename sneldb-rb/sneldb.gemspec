require_relative "lib/sneldb/version"

Gem::Specification.new do |spec|
  spec.name          = "sneldb"
  spec.version       = SnelDB::VERSION
  spec.authors       = ["Your Name"]
  spec.email         = ["your.email@example.com"]

  spec.summary       = "Ruby client for SnelDB event database"
  spec.description   = "A Ruby gem for interacting with SnelDB event database via HTTP using the standard command format"
  spec.homepage      = "https://github.com/yourusername/sneldb-rb"
  spec.license       = "MIT"

  spec.files         = Dir["lib/**/*.rb", "lib/generators/**/*", "README.md", "LICENSE"]
  spec.require_paths = ["lib"]

  spec.required_ruby_version = ">= 2.7.0"

  spec.add_dependency "openssl", "~> 3.0"
  # Arrow support is optional - users can add red-arrow gem if they want Arrow format support
  # spec.add_dependency "red-arrow", "~> 15.0"

  # Rails integration (required only if using Rails features)
  spec.add_runtime_dependency "activesupport", ">= 5.0"
  spec.add_runtime_dependency "activerecord", ">= 5.0"

  spec.add_development_dependency "bundler", "~> 2.0"
  spec.add_development_dependency "rake", "~> 13.0"
  spec.add_development_dependency "rspec", "~> 3.0"
  spec.add_development_dependency "webmock", "~> 3.0"
end

