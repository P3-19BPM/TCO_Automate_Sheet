AUDIT_FILE = os.path.join('cache', 'auditoria_copasa.json')

def carregar_auditoria():
    if os.path.exists(AUDIT_FILE):
        with open(AUDIT_FILE, 'r') as f:
            return json.load(f)
    return {}

def salvar_auditoria(bos, status):
    auditoria = carregar_auditoria()
    auditoria[bos] = status  # status pode ser 'validado_manual' ou 'invalidado_manual'
    with open(AUDIT_FILE, 'w') as f:
        json.dump(auditoria, f)

# Dentro da sua função de processamento Pandas:
def aplicar_auditoria(df):
    auditoria = carregar_auditoria()
    for idx, row in df.iterrows():
        bos = str(row['bos'])
        if bos in auditoria:
            if auditoria[bos] == 'validado_manual':
                df.at[idx, 'registro_válido'] = True
                df.at[idx, 'motivos_caso_inválido'] = "Aprovado via Auditoria"
            elif auditoria[bos] == 'invalidado_manual':
                df.at[idx, 'registro_válido'] = False
                df.at[idx, 'motivos_caso_inválido'] = "Reprovado via Auditoria"
    return df