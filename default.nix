{ pkgs ? import <nixos-unstable> {} }:

with pkgs;

buildGoModule {
  pname = "golmdb";
  version = "latest";

  nativeBuildInputs = [ pkgs.lmdb ];
  buildInputs = [ pkgs.lmdb ];

  src = with builtins; filterSource
    (path: type: substring 0 1 (baseNameOf path) != "." && (baseNameOf path) != "default.nix" && type != "symlink")
    ./.;

  vendorSha256 = "sha256:0yl6lkf49fkbjgz8365pp5f76yl8qnfj10frfkys0vx49f1nv2xh";

  meta = with lib; {
    description = "High-level Go bindings to LMDB";
    homepage = "https://fossil.wellquite.org/golmdb";
    license = licenses.openldap;
    platforms = platforms.linux ++ platforms.darwin;
  };
}
