{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-24.05";
    pre-commit-hooks = {
      url = "github:cachix/pre-commit-hooks.nix";
      inputs = {
        nixpkgs.follows = "nixpkgs";
        nixpkgs-stable.follows = "nixpkgs";
      };
    };
  };

  outputs = inputs @ {
    self,
    nixpkgs,
    ...
  }: let
    system = "x86_64-linux";
    pkgs = nixpkgs.legacyPackages.${system};
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
        ++ (with pkgs; [
          pkg-config
          zstd
          rustup
        ]);
    };

    formatter.${system} = pkgs.alejandra;
  };
}
