import polars as pl
from flask import Blueprint, jsonify, render_template, request
from datetime import datetime
from .cemig_service import buscar_dados_cemig, get_status_log
from .cemig_generator import gerar_estatisticas

cemig_bp = Blueprint('cemig', __name__, template_folder='../templates')


@cemig_bp.route('/cemig')
def painel_cemig():
    return render_template('cemig.html')


@cemig_bp.route('/api/cemig/status')
def cemig_status():
    """Retorna o status atual do processo de busca de dados (para live log no frontend)."""
    return jsonify(get_status_log())


@cemig_bp.route('/api/cemig/dados')
def get_dados_cemig():
    try:
        ano = int(request.args.get('ano', datetime.now().year))
        force_refresh = request.args.get('force_refresh', 'false').lower() == 'true'
        df, cache_info = buscar_dados_cemig(ano, force_refresh=force_refresh)

        if df.is_empty():
            return jsonify({
                "stats": [],
                "details": [],
                "cache_info": cache_info
            })

        filtro_semana = request.args.get('filtro_semana', '')
        filtro_municipio = request.args.get('filtro_municipio', '')

        df_filtrado = df
        if filtro_semana:
            df_filtrado = df_filtrado.filter(pl.col('semana') == int(filtro_semana))

        if filtro_municipio:
            df_filtrado = df_filtrado.filter(pl.col('nome_municipio') == filtro_municipio)

        stats, details = gerar_estatisticas(df_filtrado)

        return jsonify({
            'stats': stats,
            'details': details,
            'cache_info': cache_info
        })
    except Exception as e:
        print(f"[ERRO CEMIG] {e}")
        return jsonify({'error': str(e)}), 500
