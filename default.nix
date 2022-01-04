{ pkgs ? import <nixpkgs> {} }:

with pkgs;

buildGoModule {
  pname = "golmdb";
  version = "latest";

  nativeBuildInputs = [ pkgs.lmdb ];
  buildInputs = [ pkgs.lmdb ];

  src = with builtins; filterSource
    (path: type: substring 0 1 (baseNameOf path) != "." && (baseNameOf path) != "default.nix" && type != "symlink")
    ./.;

  vendorSha256 = "sha256:12yihid9r6vww8dcyx7a8k17n3yad9a48n8rxp3mnvvj5924g5zg";

  meta = with lib; {
    description = "High-level Go bindings to LMDB";
    homepage = "https://fossil.wellquite.org/golmdb";
    license = licenses.openldap;
    platforms = platforms.linux ++ platforms.darwin;
  };
}
