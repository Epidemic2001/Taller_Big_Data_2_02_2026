// Simulador del consumo 4: consulta el endpoint de ML con el escenario de los controles.
"use strict";

const $ = (id) => document.getElementById(id);
const NIVELES = [["sin daño", "#5fb36f"], ["leve", "#e3c300"], ["moderado", "#ff9100"], ["grave", "#c62828"]];
const ROMANOS = ["", "I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X", "XI", "XII"];
const fmt = (x, d = 1) => Number(x).toLocaleString("es-CL", { minimumFractionDigits: d, maximumFractionDigits: d });
const claseNivel = (n) => "nivel-" + n.replace(" ", "-").replace("ñ", "n");
const escapar = (t) => String(t).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]);

async function api(ruta, opciones) {
  const r = await fetch(ruta, opciones);
  const cuerpo = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(typeof cuerpo.detail === "string" ? cuerpo.detail : `Error ${r.status}`);
  return cuerpo;
}

function escenario() {
  return { magnitud: +$("magnitud").value, profundidad_km: +$("profundidad").value, distancia_km: +$("distancia").value };
}

function mostrarControles() {
  const e = escenario();
  $("magnitud-texto").textContent = "M" + fmt(e.magnitud);
  $("profundidad-texto").textContent = fmt(e.profundidad_km, 0) + " km";
  $("distancia-texto").textContent = fmt(e.distancia_km, 0) + " km";
}

// ------------------------------------------------------------------ resultado del escenario

function mostrarResultado(r) {
  $("resultado").innerHTML = `
    <div class="resultado">
      <div class="grado" style="background:${r.color}">${r.romano}</div>
      <div>
        <div class="valor">${fmt(r.intensidad_ems98)} <small>EMS-98</small></div>
        <div class="nombre">Grado ${r.romano} · ${escapar(r.descripcion)}</div>
        <div class="intervalo">Intervalo del 80 %: ${fmt(r.intervalo_80[0])} a ${fmt(r.intervalo_80[1])} ·
          distancia hipocentral ${fmt(r.distancia_hipocentral_km, 0)} km</div>
      </div>
    </div>
    <div class="danio"><span class="etiqueta ${claseNivel(r.nivel_danio)}">Daño ${r.nivel_danio}</span>
      ${escapar(r.danio_esperado)}</div>
    ${r.aviso ? `<div class="aviso">${escapar(r.aviso)}</div>` : ""}`;
  $("probabilidades").innerHTML = NIVELES.map(([nivel, color]) => {
    const p = r.probabilidad_danio[nivel];
    return `<div class="barra"><span>${nivel}</span>
      <div class="pista"><div class="relleno" style="width:${p * 100}%;background:${color}"></div></div>
      <span>${fmt(p * 100, 0)} %</span></div>`;
  }).join("");
}

// ------------------------------------------------------------------ curva de atenuación (SVG)

function dibujarCurva(curva, distancia) {
  const svg = $("curva");
  const W = 760, H = 330, m = { izq: 46, der: 16, arr: 14, aba: 40 };
  const puntos = curva.puntos;
  const xMax = puntos[puntos.length - 1].distancia_km;
  const yMax = Math.max(9, Math.ceil(Math.max(...puntos.map((p) => p.intervalo_80[1]))));
  const x = (d) => m.izq + (d / xMax) * (W - m.izq - m.der);
  const y = (i) => H - m.aba - ((i - 1) / (yMax - 1)) * (H - m.arr - m.aba);
  let s = "";
  // franjas de daño (VI leve, VII moderado, VIII+ grave)
  [[6, 7, "#fff8c5"], [7, 8, "#ffe5c2"], [8, yMax, "#fdecea"]].forEach(([a, b, c]) => {
    s += `<rect x="${m.izq}" y="${y(b)}" width="${W - m.izq - m.der}" height="${y(a) - y(b)}" fill="${c}"/>`;
  });
  for (let i = 1; i <= yMax; i++) {
    s += `<line x1="${m.izq}" x2="${W - m.der}" y1="${y(i)}" y2="${y(i)}" stroke="#e6e9ef"/>`;
    s += `<text x="${m.izq - 8}" y="${y(i) + 4}" text-anchor="end">${ROMANOS[i]}</text>`;
  }
  const paso = xMax > 600 ? 200 : xMax > 250 ? 50 : 25;
  for (let d = 0; d <= xMax + 1e-9; d += paso) {
    s += `<line x1="${x(d)}" x2="${x(d)}" y1="${H - m.aba}" y2="${H - m.aba + 4}" stroke="#9aa3b2"/>`;
    s += `<text x="${x(d)}" y="${H - m.aba + 17}" text-anchor="middle">${d}</text>`;
  }
  s += `<text x="${(W + m.izq) / 2}" y="${H - 6}" text-anchor="middle">Distancia al epicentro (km)</text>`;
  s += `<text x="12" y="${(H - m.aba) / 2}" transform="rotate(-90 12 ${(H - m.aba) / 2})" text-anchor="middle">Intensidad EMS-98</text>`;
  [["leve", 6.5], ["moderado", 7.5], ["grave", Math.min(8.5, yMax - 0.3)]].forEach(([t, i]) => {
    s += `<text x="${W - m.der - 6}" y="${y(i) + 4}" text-anchor="end" style="font-weight:600">daño ${t}</text>`;
  });
  // banda del 80 % y línea (punteada donde el escenario sale del rango de entrenamiento)
  const arriba = puntos.map((p) => `${x(p.distancia_km)},${y(p.intervalo_80[1])}`);
  const abajo = puntos.map((p) => `${x(p.distancia_km)},${y(p.intervalo_80[0])}`).reverse();
  s += `<polygon points="${arriba.concat(abajo).join(" ")}" fill="rgba(106,63,192,0.18)"/>`;
  // un trazo por cada tramo continuo dentro o fuera del rango, para que el punteado se vea parejo
  let tramo = [puntos[0]];
  const cerrar = () => {
    const fuera = tramo.some((p) => p.fuera_de_rango);
    s += `<polyline points="${tramo.map((p) => `${x(p.distancia_km)},${y(p.intensidad_ems98)}`).join(" ")}"
           fill="none" stroke="#6a3fc0" stroke-width="3" stroke-linejoin="round"
           ${fuera ? 'stroke-dasharray="8 6"' : 'stroke-linecap="round"'}/>`;
  };
  for (let k = 1; k < puntos.length; k++) {
    tramo.push(puntos[k]);
    if (k === puntos.length - 1 || puntos[k].fuera_de_rango !== puntos[k + 1].fuera_de_rango) {
      cerrar();
      tramo = [puntos[k]];
    }
  }
  // el escenario elegido
  const cercano = puntos.reduce((a, b) => Math.abs(b.distancia_km - distancia) < Math.abs(a.distancia_km - distancia) ? b : a);
  s += `<line x1="${x(distancia)}" x2="${x(distancia)}" y1="${m.arr}" y2="${H - m.aba}" stroke="#1d2433" stroke-dasharray="3 3"/>`;
  s += `<circle cx="${x(distancia)}" cy="${y(cercano.intensidad_ems98)}" r="6" fill="#fff" stroke="#1d2433" stroke-width="2.5"/>`;
  s += `<line x1="${m.izq}" x2="${W - m.der}" y1="${H - m.aba}" y2="${H - m.aba}" stroke="#9aa3b2"/>`;
  svg.innerHTML = s;
  $("curva-titulo").textContent = `M${fmt(curva.magnitud)} a ${fmt(curva.profundidad_km, 0)} km de profundidad. `
    + "Las franjas de color marcan desde dónde se espera daño leve, moderado y grave.";
}

// ------------------------------------------------------------------ ciudades

function mostrarCiudades(datos) {
  const filas = datos.ciudades.map((c) => `
    <tr>
      <td><span class="punto" style="background:${c.color}"></span>${escapar(c.ciudad)}</td>
      <td>${escapar(c.region)}</td>
      <td class="num">${fmt(c.distancia_km, 0)} km</td>
      <td class="num">${fmt(c.intensidad_ems98)}</td>
      <td>${c.romano} · ${escapar(c.descripcion)}</td>
      <td><span class="etiqueta ${claseNivel(c.nivel_danio)}">${c.nivel_danio}</span></td>
      <td class="num">${fmt((1 - c.probabilidad_danio["sin daño"]) * 100, 0)} %</td>
    </tr>`).join("");
  $("ciudades").innerHTML = `<table>
    <thead><tr><th>Ciudad</th><th>Región</th><th class="num">Distancia</th><th class="num">Intensidad</th>
      <th>Grado EMS-98</th><th>Daño</th><th class="num">P(daño)</th></tr></thead>
    <tbody>${filas}</tbody></table>`;
}

// ------------------------------------------------------------------ modelo

function mostrarModelo(m) {
  const c = m.corte_datos, d = m.dominio, v = m.validacion.metricas;
  const filas = Object.entries(v).map(([nombre, x]) => `
    <tr class="${nombre === m.algoritmo ? "elegido" : ""}">
      <td>${nombre}${nombre === m.algoritmo ? " (elegido)" : ""}</td>
      <td class="num">${fmt(x.mae, 2)}</td><td class="num">${fmt(x.rmse, 2)}</td>
      <td class="num">${x.r2 === null ? "—" : fmt(x.r2, 2)}</td>
      <td class="num">${fmt(x.acierto_grado * 100, 0)} %</td>
      <td class="num">${fmt(x.acierto_grado_mas_menos_1 * 100, 0)} %</td></tr>`).join("");
  $("modelo").innerHTML = `
    <dl class="ficha">
      <dt>Versión</dt><dd><code>${m.version}</code> · entrenada ${m.entrenado_utc.replace("T", " ").slice(0, 16)} UTC</dd>
      <dt>Algoritmo</dt><dd>${escapar(m.descripcion)}</dd>
      <dt>Datos (corte)</dt><dd>${c.testimonios.toLocaleString("es-CL")} testimonios de ${c.sismos} sismos,
        del ${c.desde_utc.slice(0, 10)} al ${c.hasta_utc.slice(0, 10)} · ${escapar(m.fuente)}</dd>
      <dt>Rango de entrenamiento</dt><dd>M${d.magnitud[0]}–${d.magnitud[1]}, profundidad ${d.profundidad_km[0]}–${d.profundidad_km[1]} km,
        distancia hasta ${fmt(d.distancia_km[1], 0)} km</dd>
      <dt>Ecuación de atenuación</dt><dd><code>${escapar(m.ecuacion_atenuacion.formula)}</code></dd>
    </dl>
    <table>
      <thead><tr><th>Validación: ${escapar(m.validacion.metodo)}</th><th class="num">MAE</th><th class="num">RMSE</th>
        <th class="num">R²</th><th class="num">Grado exacto</th><th class="num">±1 grado</th></tr></thead>
      <tbody>${filas}</tbody></table>`;
}

async function cargarModelo() {
  try {
    mostrarModelo(await api("/api/modelo"));
    return true;
  } catch (e) {
    $("modelo").innerHTML = `<div class="error">${escapar(e.message)}</div>`;
    return false;
  }
}

async function evaluar() {
  $("btn-evaluar").disabled = true;
  try {
    const e = await api("/api/modelo/evaluacion");
    const m = e.metricas_nuevos;
    $("evaluacion").innerHTML = `<div class="veredicto ${e.reentrenar ? "si" : ""}">
      <strong>${e.reentrenar ? "Conviene reentrenar" : "No hace falta reentrenar"}</strong>
      · ${e.testimonios_nuevos} testimonios posteriores al corte (${e.corte_utc.slice(0, 16)} UTC)${m ? `,
      MAE ${fmt(m.mae, 2)} frente a ${fmt(e.mae_validacion, 2)} en la validación` : ""}.
      <ul>${e.motivos.map((t) => `<li>${escapar(t)}</li>`).join("")}</ul></div>`;
  } catch (err) {
    $("evaluacion").innerHTML = `<div class="error">${escapar(err.message)}</div>`;
  } finally {
    $("btn-evaluar").disabled = false;
  }
}

async function reentrenar() {
  $("btn-reentrenar").disabled = true;
  $("evaluacion").innerHTML = `<p class="ayuda">Entrenando con los testimonios de ClickHouse…</p>`;
  try {
    const r = await api("/api/modelo/reentrenar", { method: "POST" });
    mostrarModelo(r.modelo);
    $("evaluacion").innerHTML = `<div class="veredicto">Nueva versión <code>${r.version_nueva}</code>
      ${r.version_anterior ? `(reemplaza a <code>${r.version_anterior}</code>)` : ""}.</div>`;
    actualizar();
  } catch (err) {
    $("evaluacion").innerHTML = `<div class="error">${escapar(err.message)}</div>`;
  } finally {
    $("btn-reentrenar").disabled = false;
  }
}

// ------------------------------------------------------------------ actualización

let pendiente = null;
async function actualizar() {
  mostrarControles();
  const e = escenario();
  const q = (o) => new URLSearchParams(o).toString();
  const base = { magnitud: e.magnitud, profundidad_km: e.profundidad_km };
  try {
    const [r, curva, ciudades] = await Promise.all([
      api("/api/predecir?" + q(e)),
      api("/api/curva-atenuacion?" + q({ ...base, hasta_km: 400, puntos: 81 })),
      api("/api/ciudades?" + q({ ...base, latitud: +$("lat").value, longitud: +$("lon").value })),
    ]);
    mostrarResultado(r);
    dibujarCurva(curva, e.distancia_km);
    mostrarCiudades(ciudades);
  } catch (err) {
    $("resultado").innerHTML = `<div class="error">${escapar(err.message)}</div>`;
  }
}
const programar = () => { mostrarControles(); clearTimeout(pendiente); pendiente = setTimeout(actualizar, 120); };

["magnitud", "profundidad", "distancia", "lat", "lon"].forEach((id) => $(id).addEventListener("input", programar));
$("preset").addEventListener("change", () => {
  if (!$("preset").value) return;
  const [lat, lon] = $("preset").value.split(",");
  $("lat").value = lat; $("lon").value = lon;
  actualizar();
});
$("btn-evaluar").addEventListener("click", evaluar);
$("btn-reentrenar").addEventListener("click", reentrenar);

mostrarControles();
cargarModelo().then((hay) => (hay ? actualizar() : ($("resultado").innerHTML =
  `<p class="ayuda">Sin modelo todavía: carguen testimonios en ClickHouse y usen «Reentrenar ahora».</p>`)));
