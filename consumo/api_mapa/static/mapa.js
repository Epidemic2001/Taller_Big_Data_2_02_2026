// Mapa web del consumo "API REST + mapa": todo lo que muestra lo pide a la API (/api/...).
"use strict";

const CALAMA = [-22.456, -68.929];
const $ = (id) => document.getElementById(id);
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const num = (v, d = 1) => (v === null || v === undefined ? "—" : Number(v).toLocaleString("es-CL", { minimumFractionDigits: d, maximumFractionDigits: d }));
const ROMANOS = ["", "I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X+"];
const LETRA = { "hospital": "H", "clínica": "C", "subestación eléctrica": "S", "faena minera": "M" };

const estado = { evento: null, origen: null, destino: null, modo: null, pedido: 0 };

// ------------------------------------------------------------------ mapa base y capas
const mapa = L.map("mapa", { zoomControl: true }).setView(CALAMA, 12);
L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
  maxZoom: 19,
  attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> · ShakeMap USGS',
}).addTo(mapa);
for (const [nombre, z] of [["isosistas", 405], ["sectores", 410], ["calles", 420], ["rutas", 430]]) {
  mapa.createPane(nombre).style.zIndex = z;
}
const capas = {
  isosistas: L.layerGroup(),
  sectores: L.layerGroup().addTo(mapa),
  bloqueadas: L.layerGroup().addTo(mapa),
  calles: L.layerGroup(),
  infra: L.layerGroup().addTo(mapa),
  rutas: L.layerGroup().addTo(mapa),
};
const casillas = { "capa-isosistas": "isosistas", "capa-sectores": "sectores", "capa-bloqueadas": "bloqueadas",
  "capa-calles": "calles", "capa-infra": "infra" };
for (const [id, capa] of Object.entries(casillas)) {
  $(id).addEventListener("change", (e) => (e.target.checked ? capas[capa].addTo(mapa) : capas[capa].remove()));
}

async function api(ruta) {
  const r = await fetch(ruta);
  const cuerpo = await r.json().catch(() => ({}));
  if (!r.ok) {
    const detalle = Array.isArray(cuerpo.detail) ? cuerpo.detail.map((d) => d.msg).join("; ") : cuerpo.detail;
    throw new Error(detalle || `Error ${r.status} en ${ruta}`);
  }
  return cuerpo;
}

const chip = (p) => `<span class="chip"><i style="background:${p.color}">${esc(p.romano)}</i>${esc(p.percepcion)} · daño ${esc(String(p.dano).toLowerCase())}</span>`;
const umbral = () => Number($("umbral").value);

// ------------------------------------------------------------------ sismos
async function cargarEventos() {
  try {
    const eventos = await api("/api/eventos");
    const sel = $("evento");
    if (!eventos.length) {
      sel.innerHTML = "<option>Sin sismos cargados</option>";
      $("ficha").innerHTML = `<div class="aviso">PostGIS no tiene ShakeMaps. Cárgalos con <code>herramientas/lambda/invocar.py</code>.</div>`;
      return;
    }
    sel.innerHTML = eventos.map((e) => {
      const fecha = e.tiempo_utc ? e.tiempo_utc.slice(0, 10) : "";
      const mmi = e.shakemap_cubre_calama ? `MMI ${e.intensidad_calama.romano} en Calama` : "fuera de Calama";
      return `<option value="${esc(e.event_id)}">M${num(e.magnitud)} · ${esc(fecha)} · ${mmi}</option>`;
    }).join("");
    sel.addEventListener("change", () => elegirEvento(sel.value));
    elegirEvento(eventos[0].event_id);
  } catch (err) {
    $("ficha").innerHTML = `<div class="aviso error">${esc(err.message)}</div>`;
  }
}

async function elegirEvento(id) {
  estado.evento = id;
  $("enlace-cog").href = `/api/eventos/${encodeURIComponent(id)}/cog`;
  const e = await api(`/api/eventos/${encodeURIComponent(id)}`);
  const fecha = e.tiempo_utc ? new Date(e.tiempo_utc).toLocaleString("es-CL", { timeZone: "America/Santiago", dateStyle: "medium", timeStyle: "short" }) : "—";
  $("ficha").innerHTML = `
    <div class="ficha">
      <div class="magnitud">${num(e.magnitud)}<small>magnitud</small></div>
      <div class="lugar">${esc(e.lugar)}</div>
      <div class="dato">${esc(fecha)} · ${num(e.profundidad_km, 0)} km de profundidad</div>
      <div class="dato">A ${num(e.distancia_calama_km, 0)} km de Calama · MMI máxima ${num(e.mmi_max)}</div>
    </div>
    ${e.shakemap_cubre_calama
      ? `<div class="intensidad-calama"><strong>Intensidad en la plaza de Calama: ${num(e.mmi_calama)}</strong>${chip(e.intensidad_calama)}</div>`
      : `<div class="aviso">El ShakeMap de este sismo no cubre Calama: no hay intensidad para la ciudad.</div>`}`;
  await Promise.all([cargarIsosistas(), cargarSectores(), cargarInfra(), cargarCalles(), cargarResumen()]);
  if (estado.origen && estado.destino) calcularRuta();
}

// ------------------------------------------------------------------ capas de daño
function clicEnCapa(capa) {
  // con un modo de marcado activo, un clic sobre un polígono marca el punto en vez de abrir su ficha
  capa.on("click", (ev) => {
    if (estado.modo) {
      L.DomEvent.stop(ev);
      colocar(ev.latlng);
    }
  });
}

async function cargarIsosistas() {
  const datos = await api(`/api/eventos/${encodeURIComponent(estado.evento)}/isosistas`);
  capas.isosistas.clearLayers();
  const capa = L.geoJSON(datos, {
    pane: "isosistas",
    bubblingMouseEvents: false,
    style: (f) => ({ color: "#455a64", weight: 0.6, fillColor: f.properties.color, fillOpacity: 0.35 }),
    onEachFeature: (f, l) => l.bindTooltip(`MMI ${esc(f.properties.romano)} · ${esc(f.properties.percepcion)} · daño ${esc(f.properties.dano.toLowerCase())}`, { sticky: true }),
  });
  clicEnCapa(capa);
  capas.isosistas.addLayer(capa);
}

async function cargarSectores() {
  const datos = await api(`/api/eventos/${encodeURIComponent(estado.evento)}/sectores`);
  capas.sectores.clearLayers();
  const capa = L.geoJSON(datos, {
    pane: "sectores",
    bubblingMouseEvents: false,
    style: (f) => ({ color: "#37474f", weight: 1.2, fillColor: f.properties.color, fillOpacity: 0.5 }),
    onEachFeature: (f, l) => {
      const p = f.properties;
      l.bindTooltip(`${esc(p.nombre)} · MMI ${esc(p.romano)}`, { sticky: true });
      l.bindPopup(`<strong>${esc(p.nombre)}</strong> <span class="ejemplos">(${esc(p.tipo)})</span><br>${chip(p)}
        <table><tr><td>MMI promedio</td><td>${num(p.mmi_prom, 2)}</td></tr>
        <tr><td>MMI máxima</td><td>${num(p.mmi_max, 2)}</td></tr>
        <tr><td>Superficie</td><td>${num(p.area_km2, 2)} km²</td></tr>
        ${p.poblacion ? `<tr><td>Población (OSM)</td><td>${num(p.poblacion, 0)}</td></tr>` : ""}</table>`);
    },
  });
  capa.on("popupopen", (ev) => { if (estado.modo) ev.popup.close(); });
  clicEnCapa(capa);
  capas.sectores.addLayer(capa);
}

async function cargarInfra() {
  const datos = await api(`/api/eventos/${encodeURIComponent(estado.evento)}/infraestructura`);
  capas.infra.clearLayers();
  capas.infra.addLayer(L.geoJSON(datos, {
    pointToLayer: (f, latlng) => L.marker(latlng, {
      icon: L.divIcon({ className: "", iconSize: [24, 24], iconAnchor: [12, 12],
        html: `<div class="icono-infra" style="background:${f.properties.color}">${LETRA[f.properties.tipo] || "?"}</div>` }),
      title: f.properties.nombre || f.properties.tipo,
    }),
    onEachFeature: (f, l) => {
      const p = f.properties;
      l.bindPopup(`<strong>${esc(p.nombre || "Sin nombre")}</strong><br><span class="ejemplos">${esc(p.tipo)}</span><br>${chip(p)}
        <table><tr><td>MMI</td><td>${num(p.mmi, 2)}</td></tr>
        <tr><td>Aceleración (PGA)</td><td>${num(p.pga_g, 3)} g</td></tr>
        <tr><td>Velocidad (PGV)</td><td>${num(p.pgv_cm_s, 1)} cm/s</td></tr>
        <tr><td>Distancia al epicentro</td><td>${num(p.distancia_epicentro_km, 0)} km</td></tr></table>`);
    },
  }));
}

async function cargarCalles() {
  const datos = await api(`/api/eventos/${encodeURIComponent(estado.evento)}/red-vial?evitar_mmi=${umbral()}`);
  capas.calles.clearLayers();
  capas.bloqueadas.clearLayers();
  for (const f of datos.features) {
    const p = f.properties;
    const lineas = f.geometry.coordinates.map((l) => l.map(([lon, lat]) => [lat, lon]));
    // intensidad por calle (capa opcional): línea del color del grado sobre un borde oscuro
    L.polyline(lineas, { pane: "calles", color: "#37474f", weight: 4.5, opacity: 0.6, interactive: false }).addTo(capas.calles);
    L.polyline(lineas, { pane: "calles", color: p.color, weight: 2.5 })
      .bindTooltip(`${num(p.km)} km de calles con MMI ${esc(p.romano)}`, { sticky: true }).addTo(capas.calles);
    if (p.bloqueado) {
      L.polyline(lineas, { pane: "calles", color: "#b71c1c", weight: 4, dashArray: "6 5" })
        .bindTooltip(`Calle bloqueada para la ruta segura: MMI ${esc(p.romano)} ≥ ${num(datos.evitar_mmi)}`, { sticky: true })
        .addTo(capas.bloqueadas);
    }
  }
}

async function cargarResumen() {
  const r = await api(`/api/eventos/${encodeURIComponent(estado.evento)}/resumen?evitar_mmi=${umbral()}`);
  const filaSector = (s) => `<tr><td><span class="chip"><i style="background:${s.color}"></i>${esc(s.dano)}</span>
      <div class="ejemplos">${s.ejemplos.map(esc).join(", ")}${s.sectores > s.ejemplos.length ? "…" : ""}</div></td>
      <td class="num">${s.sectores}</td><td class="num">${s.poblacion ? num(s.poblacion, 0) : "—"}</td></tr>`;
  const filaInfra = (i) => `<tr><td>${esc(i.tipo)}</td><td>${esc(i.dano)}</td><td class="num">${i.instalaciones}</td></tr>`;
  const v = r.red_vial;
  const grados = Object.entries(v.km_por_grado).map(([g, km]) => `MMI ${esc(g)}: ${num(km)} km`).join(" · ");
  $("resumen").innerHTML = `
    <table><thead><tr><th>Daño potencial por sector</th><th class="num">Sectores</th><th class="num">Población</th></tr></thead>
      <tbody>${r.sectores_por_dano.map(filaSector).join("") || '<tr><td colspan="3">Sin sectores cargados</td></tr>'}</tbody></table>
    <div class="subtitulo">Instalaciones críticas</div>
    <table><thead><tr><th>Tipo</th><th>Daño potencial</th><th class="num">N°</th></tr></thead>
      <tbody>${r.infraestructura_por_dano.map(filaInfra).join("") || '<tr><td colspan="3">Sin instalaciones cargadas</td></tr>'}</tbody></table>
    <div class="subtitulo">Red vial (${num(v.km_total, 0)} km)</div>
    <div class="ejemplos">${grados || "El ShakeMap no cubre la red vial"}</div>
    <div class="aviso ${v.km_bloqueados ? "error" : "info"}">${num(v.km_bloqueados)} km de calles con MMI ≥ ${num(v.evitar_mmi)} quedan bloqueadas para la ruta segura.</div>`;
}

// ------------------------------------------------------------------ rutas
const marcadores = {};

function icono(letra) {
  return L.divIcon({ className: "", iconSize: [28, 28], iconAnchor: [14, 30], html: `<div class="icono-punto"><span>${letra}</span></div>` });
}

function fijarModo(modo) {
  estado.modo = estado.modo === modo ? null : modo;
  $("btn-origen").classList.toggle("activo", estado.modo === "origen");
  $("btn-destino").classList.toggle("activo", estado.modo === "destino");
  mapa.getContainer().classList.toggle("marcando", Boolean(estado.modo));
  // en el celular el mapa queda arriba del panel: se lleva a la vista para poder marcar el punto
  if (estado.modo && window.matchMedia("(max-width: 820px)").matches && window.scrollY > 0) {
    $("mapa").scrollIntoView({ behavior: "smooth", block: "start" });
  }
}

function ponerMarcador(cual, latlng) {
  estado[cual] = latlng;
  if (marcadores[cual]) marcadores[cual].setLatLng(latlng);
  else {
    marcadores[cual] = L.marker(latlng, { icon: icono(cual === "origen" ? "A" : "B"), draggable: true, zIndexOffset: 1000 })
      .addTo(mapa)
      .on("dragend", (ev) => { estado[cual] = ev.target.getLatLng(); if (estado.origen && estado.destino) calcularRuta(); });
  }
  $("btn-hospital").disabled = !estado.origen;
}

function colocar(latlng) {
  const cual = estado.modo;
  ponerMarcador(cual, latlng);
  fijarModo(null);
  if (cual === "origen" && !estado.destino) fijarModo("destino");
  if (estado.origen && estado.destino) calcularRuta();
}

mapa.on("click", (ev) => { if (estado.modo) colocar(ev.latlng); });
$("btn-origen").addEventListener("click", () => fijarModo("origen"));
$("btn-destino").addEventListener("click", () => fijarModo("destino"));
$("btn-limpiar").addEventListener("click", () => {
  for (const cual of ["origen", "destino"]) { if (marcadores[cual]) { marcadores[cual].remove(); delete marcadores[cual]; } estado[cual] = null; }
  capas.rutas.clearLayers();
  $("resultado-ruta").innerHTML = "";
  $("btn-hospital").disabled = true;
  fijarModo(null);
});
$("btn-hospital").addEventListener("click", rutaHospital);

const punto = (ll) => `${ll.lat.toFixed(6)},${ll.lng.toFixed(6)}`;

function dibujar(feature, tipo) {
  const latlngs = feature.geometry.coordinates.map(([lon, lat]) => [lat, lon]);
  // tramo a pie desde cada marcador hasta donde la ruta entra a la red vial
  for (const [marca, extremo] of [[estado.origen, latlngs[0]], [estado.destino, latlngs[latlngs.length - 1]]]) {
    if (marca && tipo === "segura") {
      L.polyline([marca, extremo], { pane: "rutas", color: "#263238", weight: 2, dashArray: "2 6", interactive: false }).addTo(capas.rutas);
    }
  }
  if (tipo === "directa") {
    L.polyline(latlngs, { pane: "rutas", color: "#37474f", weight: 4, dashArray: "8 8", opacity: 0.9 }).addTo(capas.rutas);
  } else {
    L.polyline(latlngs, { pane: "rutas", color: "#fff", weight: 9, opacity: 0.9, interactive: false }).addTo(capas.rutas);
    L.polyline(latlngs, { pane: "rutas", color: "#1565c0", weight: 5 }).addTo(capas.rutas);
  }
  return latlngs;
}

function filasComparacion(seg, dir) {
  const fila = (nombre, campo, d, unidad = "") => `<tr><td>${nombre}</td><td class="num">${num(seg[campo], d)}${unidad}</td>${dir ? `<td class="num">${num(dir[campo], d)}${unidad}</td>` : ""}</tr>`;
  return `<table class="comparacion"><thead><tr><th></th><th class="num">Segura</th>${dir ? '<th class="num">Directa</th>' : ""}</tr></thead><tbody>
    ${fila("Distancia", "distancia_km", 1, " km")}
    ${fila("Tiempo estimado", "tiempo_min", 0, " min")}
    ${fila("MMI máxima en el camino", "mmi_max", 1)}
    ${fila("Km con MMI ≥ VI (daño leve o más)", "km_con_dano_potencial", 1, " km")}
    ${fila(`Km por calles bloqueadas (MMI ≥ ${num(umbral())})`, "km_sobre_umbral", 1, " km")}
  </tbody></table>`;
}

async function calcularRuta() {
  if (!estado.evento || !estado.origen || !estado.destino) return;
  const pedido = ++estado.pedido;
  $("resultado-ruta").innerHTML = '<p class="ayuda">Calculando…</p>';
  try {
    const r = await api(`/api/eventos/${encodeURIComponent(estado.evento)}/ruta?desde=${punto(estado.origen)}&hasta=${punto(estado.destino)}&evitar_mmi=${umbral()}`);
    if (pedido !== estado.pedido) return;
    capas.rutas.clearLayers();
    const [seg, dir] = r.features;
    const lineaDirecta = dibujar(dir, "directa");  // primero, para que la segura quede encima
    const limites = L.latLngBounds(dibujar(seg, "segura")).extend(lineaDirecta).extend([estado.origen, estado.destino]);
    mapa.fitBounds(limites, { padding: [40, 40], maxZoom: 15 });
    const notas = [];
    if (!r.shakemap_cubre_ruta) notas.push('<div class="aviso">El ShakeMap no cubre estas calles: las rutas solo consideran el tiempo de viaje.</div>');
    else if (r.rutas_coinciden) notas.push(`<div class="aviso info">La ruta segura coincide con la directa: ninguna calle del camino alcanza MMI ${num(umbral())} y no hay un desvío con menor intensidad que compense el tiempo extra.</div>`);
    if (r.aviso) notas.push(`<div class="aviso error">${esc(r.aviso)}</div>`);
    const calles = seg.properties.calles_principales.length ? `<p class="ejemplos">Por: ${seg.properties.calles_principales.map(esc).join(", ")}</p>` : "";
    $("resultado-ruta").innerHTML = filasComparacion(seg.properties, dir.properties) + calles + notas.join("");
  } catch (err) {
    if (pedido !== estado.pedido) return;
    capas.rutas.clearLayers();
    $("resultado-ruta").innerHTML = `<div class="aviso error">${esc(err.message)}</div>`;
  }
}

async function rutaHospital() {
  if (!estado.evento || !estado.origen) return;
  const pedido = ++estado.pedido;
  $("resultado-ruta").innerHTML = '<p class="ayuda">Buscando el hospital operativo más cercano…</p>';
  try {
    const r = await api(`/api/eventos/${encodeURIComponent(estado.evento)}/ruta-hospital?desde=${punto(estado.origen)}&evitar_mmi=${umbral()}`);
    if (pedido !== estado.pedido) return;
    capas.rutas.clearLayers();
    const ruta = r.features[0];
    const d = ruta.properties.destino;
    if (marcadores.destino) { marcadores.destino.remove(); delete marcadores.destino; estado.destino = null; }
    mapa.fitBounds(L.latLngBounds(dibujar(ruta, "segura")).extend(estado.origen).extend([d.lat, d.lon]), { padding: [40, 40], maxZoom: 15 });
    const alternativas = r.alternativas.length
      ? `<div class="subtitulo">Otros centros alcanzables</div><table><tbody>${r.alternativas.map((a) =>
        `<tr><td>${esc(a.nombre || a.tipo)}</td><td>MMI ${esc(a.romano)}</td><td class="num">${num(a.tiempo_min, 0)} min</td></tr>`).join("")}</tbody></table>` : "";
    const descartados = r.descartados.length
      ? `<div class="aviso">Descartados por haber recibido MMI ≥ ${num(umbral())} (posible daño): ${r.descartados.map((x) => esc(x.nombre || x.tipo)).join(", ")}.</div>` : "";
    $("resultado-ruta").innerHTML = `
      <div class="destino"><strong>${esc(d.nombre || d.tipo)}</strong>${esc(d.tipo)} · recibió ${chip(d)}</div>
      ${filasComparacion(ruta.properties, null)}
      ${r.aviso ? `<div class="aviso error">${esc(r.aviso)}</div>` : ""}${descartados}${alternativas}`;
  } catch (err) {
    if (pedido !== estado.pedido) return;
    capas.rutas.clearLayers();
    $("resultado-ruta").innerHTML = `<div class="aviso error">${esc(err.message)}</div>`;
  }
}

// ------------------------------------------------------------------ umbral y leyenda
function textoUmbral() {
  const v = umbral();
  $("umbral-texto").textContent = `MMI ${num(v)} (${ROMANOS[Math.min(10, Math.floor(v))]})`;
}
let espera;
$("umbral").addEventListener("input", () => {
  textoUmbral();
  clearTimeout(espera);
  espera = setTimeout(() => {
    if (!estado.evento) return;
    cargarCalles(); cargarResumen();
    if (estado.origen && estado.destino) calcularRuta();
  }, 350);
});
textoUmbral();

async function cargarLeyenda() {
  const escala = await api("/api/escala");
  $("leyenda").innerHTML = escala.filter((g) => g.grado >= 4).map((g) =>
    `<span class="chip"><i style="background:${g.color}">${esc(g.romano)}</i></span><span>${esc(g.percepcion)} · daño ${esc(g.dano.toLowerCase())}</span>`).join("")
    + '<span class="linea segura"></span><span>Ruta segura</span>'
    + '<span class="linea directa"></span><span>Ruta directa (sin considerar el sismo)</span>'
    + '<span class="linea bloqueada"></span><span>Calle bloqueada (MMI sobre el umbral)</span>'
    + '<span class="chip"><i style="background:#fff">H</i></span><span>Hospital · C clínica · S subestación · M faena</span>';
}

cargarLeyenda().catch(() => {});
cargarEventos();
