{
  inputs = {
    nixpkgs.url = "flake:nixpkgs";
    pre-commit-hooks = {
      url = "github:cachix/pre-commit-hooks.nix";
      inputs = {
        nixpkgs.follows = "nixpkgs";
        nixpkgs-stable.follows = "nixpkgs";
      };
    };
    crane.url = "github:ipetkov/crane";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.rust-analyzer-src.follows = "";
    };
  };

  outputs = inputs @ {
    self,
    nixpkgs,
    crane,
    fenix,
    ...
  }: let
    system = "x86_64-linux";
    pkgs = nixpkgs.legacyPackages.${system};
    craneLib = (crane.mkLib pkgs).overrideToolchain (fenix.packages.${system}.stable.withComponents ["cargo" "rustc"]);

    scraperSysDeps = with pkgs; [
      pkg-config
      zstd
      openssl
    ];
  in {
    checks.${system}.pre-commit-check = inputs.pre-commit-hooks.lib.${system}.run {
      src = ./.;
      hooks = {
        # rust
        clippy = {
          enable = true;
          packageOverrides = {
            cargo = pkgs.rustup;
            clippy = pkgs.rustup;
          };
        };
        rustfmt = {
          enable = true;
          packageOverrides = {
            cargo = pkgs.rustup;
            rustfmt = pkgs.rustup;
          };
        };

        # nix
        alejandra.enable = true;
        deadnix.enable = true;

        # Git
        check-merge-conflicts.enable = true;
        forbid-new-submodules.enable = true;

        typos = {
          enable = true;
          settings.configPath = "./typos.toml";
        };
      };
    };

    devShells.${system}.default = nixpkgs.legacyPackages.${system}.mkShell {
      inherit (self.checks.${system}.pre-commit-check) shellHook;
      buildInputs =
        self.checks.${system}.pre-commit-check.enabledPackages
        ++ scraperSysDeps
        ++ (with pkgs; [
          rustup
        ]);
    };

    packages.${system} = {
      scrape-solana = craneLib.buildPackage {
        pname = "scrape-solana";
        version = "latest";
        src = craneLib.cleanCargoSource ./.;
        buildInputs = scraperSysDeps;

        # it's a workspace, we need to select the right thing
        cargoExtraArgs = "-p scrape-solana";

        # panic on
        cargoBuildCommand = "cargo build --profile release --config debug=\\\"full\\\" --config overflow-checks=true --config lto=\\\"thin\\\"";
      };
      img-builder = pkgs.dockerTools.streamLayeredImage {
        name = "scrape-solana";
        tag = "latest";

        extraCommands = ''
          mkdir -p data
        '';

        config = {
          Entrypoint = ["${self.packages.${system}.scrape-solana}/bin/scrape-solana"];
          WorkingDir = "/data";
        };
      };
    };

    formatter.${system} = pkgs.alejandra;
  };
}
