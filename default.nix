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

  vendorSha256 = "sha256-KnMFg7O5sTp2pupGajyUHlyqDcAp+oqfyO4sP93mBMc=";

  meta = with lib; {
    description = "High-level Go bindings to LMDB";
    homepage = "https://fossil.wellquite.org/golmdb";
    license = licenses.openldap;
    platforms = platforms.linux ++ platforms.darwin;
  };
}
