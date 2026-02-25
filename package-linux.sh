#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

APP_NAME="Puppy DB Tool"
PKG_NAME="puppy-db-tool"
BIN_NAME="puppy-db-tool"
VERSION="${1:-0.1.0}"
ARCH="${ARCH:-$(dpkg --print-architecture)}"

if [[ ! "$VERSION" =~ ^[0-9]+(\.[0-9]+){1,3}([~-][A-Za-z0-9.+]+)?$ ]]; then
  echo "error: invalid version '$VERSION' (example: 0.1.0)" >&2
  exit 1
fi

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "error: required command not found: $1" >&2
    exit 1
  fi
}

require_cmd dpkg-deb
require_cmd install

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  echo "Building desktop binary..."
  ./run.sh build
fi

BIN_PATH="$ROOT_DIR/build/bin/$BIN_NAME"
if [[ ! -f "$BIN_PATH" ]]; then
  echo "error: binary not found at $BIN_PATH" >&2
  exit 1
fi

DIST_DIR="$ROOT_DIR/dist"
STAGE_DIR="$DIST_DIR/${PKG_NAME}_${VERSION}_${ARCH}"
DEBIAN_DIR="$STAGE_DIR/DEBIAN"
OUT_DEB="$DIST_DIR/${PKG_NAME}_${VERSION}_${ARCH}.deb"

rm -rf "$STAGE_DIR"
mkdir -p \
  "$DEBIAN_DIR" \
  "$STAGE_DIR/usr/local/bin" \
  "$STAGE_DIR/opt/$PKG_NAME" \
  "$STAGE_DIR/usr/share/applications" \
  "$STAGE_DIR/usr/share/icons/hicolor/scalable/apps"

install -m 0755 "$BIN_PATH" "$STAGE_DIR/opt/$PKG_NAME/$BIN_NAME-bin"

cat >"$STAGE_DIR/usr/local/bin/$BIN_NAME" <<EOF
#!/usr/bin/env bash
set -euo pipefail
while IFS='=' read -r name _; do
  if [[ "\$name" == SNAP_* ]]; then
    unset "\$name"
  fi
done < <(env)
unset GTK_EXE_PREFIX GTK_PATH GTK_MODULES GTK_IM_MODULE_FILE GIO_MODULE_DIR SNAP_LIBRARY_PATH LD_PRELOAD
exec /opt/$PKG_NAME/$BIN_NAME-bin "\$@"
EOF

cat >"$DEBIAN_DIR/control" <<EOF
Package: $PKG_NAME
Version: $VERSION
Section: database
Priority: optional
Architecture: $ARCH
Maintainer: Puppy DB Tool Team <support@puppydb.local>
Depends: libgtk-3-0, libwebkit2gtk-4.1-0 | libwebkit2gtk-4.0-37, libsoup-3.0-0 | libsoup2.4-1
Description: Puppy DB Tool desktop database manager
 Linux desktop database manager for MySQL, PostgreSQL, MongoDB, and Redis
 with direct and SSH tunnel connections.
EOF

cat >"$STAGE_DIR/usr/share/applications/${PKG_NAME}.desktop" <<EOF
[Desktop Entry]
Type=Application
Version=1.0
Name=$APP_NAME
Comment=Desktop database management tool
Exec=/usr/local/bin/$BIN_NAME
Icon=$PKG_NAME
Terminal=false
Categories=Development;Database;
StartupNotify=true
EOF

cat >"$STAGE_DIR/usr/share/icons/hicolor/scalable/apps/${PKG_NAME}.svg" <<'EOF'
<svg xmlns="http://www.w3.org/2000/svg" width="256" height="256" viewBox="0 0 256 256">
  <defs>
    <linearGradient id="bg" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0%" stop-color="#1a55d8"/>
      <stop offset="100%" stop-color="#1aa2ff"/>
    </linearGradient>
  </defs>
  <rect x="16" y="16" width="224" height="224" rx="48" fill="#091631"/>
  <rect x="32" y="32" width="192" height="192" rx="36" fill="url(#bg)"/>
  <g fill="#ffffff" font-family="DejaVu Sans, Arial, sans-serif" font-weight="700" text-anchor="middle">
    <text x="128" y="118" font-size="52">DB</text>
    <text x="128" y="156" font-size="24">PUPPY</text>
  </g>
</svg>
EOF

chmod 0644 "$DEBIAN_DIR/control"
chmod 0755 "$STAGE_DIR/usr/local/bin/$BIN_NAME"
chmod 0644 "$STAGE_DIR/usr/share/applications/${PKG_NAME}.desktop"
chmod 0644 "$STAGE_DIR/usr/share/icons/hicolor/scalable/apps/${PKG_NAME}.svg"

mkdir -p "$DIST_DIR"
rm -f "$OUT_DEB"
dpkg-deb --build --root-owner-group "$STAGE_DIR" "$OUT_DEB"

echo
echo "Package created:"
echo "  $OUT_DEB"
echo
echo "Install with:"
echo "  sudo dpkg -i \"$OUT_DEB\""
