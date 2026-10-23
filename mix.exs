defmodule WardrikEvents.MixProject do
  use Mix.Project

  def project do
    [
      app: :wardrik_events,
      version: "0.1.0",
      elixir: "~> 1.20",
      description:
        "Each agent's events as CloudEvents: a log to read back, and a stream to follow.",
      package: package(),
      docs: [main: "readme", extras: ["README.md", "CHANGELOG.md"]],
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      aliases: aliases(),
      dialyzer: [
        plt_add_apps: [
          :mix,
          :ex_unit,
          :ekv,
          :ecto,
          :ecto_sql,
          :postgrex,
          :db_connection,
          :durable_server
        ],
        plt_core_path: "priv/plts",
        plt_local_path: "priv/plts"
      ],
      deps: deps()
    ]
  end

  def cli, do: [preferred_envs: [precommit: :test]]

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => "https://github.com/wardrik-dev/wardrik_events"},
      files: ~w(lib mix.exs README.md CHANGELOG.md LICENSE)
    ]
  end

  defp aliases do
    [
      precommit: [
        "deps.unlock --check-unused",
        "format --check-formatted",
        "credo --strict",
        "ex_dna --max-clones 0",
        "reach.check --arch --smells --strict",
        "test --warnings-as-errors --cover --export-coverage default",
        "test.coverage",
        "crap",
        "dialyzer --force-check"
      ]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:wardrik_core, path: "../wardrik_core"},
      {:wardrik_store, path: "../wardrik_store"},
      {:wardrik_protocol, path: "../wardrik_protocol"},
      {:group, "~> 0.2"},
      {:mimic, "~> 2.4", only: :test},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:ex_slop, "~> 0.4", only: [:dev, :test], runtime: false},
      {:ex_dna, "~> 1.5", only: [:dev, :test], runtime: false},
      {:ex_crap, "~> 0.1.0", only: [:dev, :test], runtime: false},
      {:reach, "~> 2.8", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end
end
