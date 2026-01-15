# Invoice Mapping Guide - 32H (Enartia) → NetSuite

## 📋 Overview

Este documento mapeia os campos da API Entersoft (32H - Enartia S.A.) para os campos esperados pelo NetSuite, garantindo compatibilidade com o template padrão.

## 🔄 Mapeamento de Campos

### Campos de Cabeçalho (Invoice Header)

| Campo API (Source) | Campo NetSuite (Target) | Tipo | Obrigatório | Transformação | Notas |
|-------------------|------------------------|------|-------------|---------------|-------|
| `ADCode` | `invoice_id` | string | ✅ | - | Identificador único da invoice |
| `ADCode` | `invoice_number` | string | ✅ | - | Número da invoice para display |
| - | `entity` | string | ✅ | `literal('32H')` | Código fixo da entidade |
| - | `platform` | string | ✅ | `literal('Enartia')` | Nome da plataforma |
| - | `brand` | string | ✅ | `literal('Enartia')` | Nome da marca |
| - | `legal_entity` | string | ✅ | `literal('Enartia S.A.')` | Entidade legal |
| `ADRegistrationDate` | `invoice_date` | date | ✅ | `convert_date` | Data da transação |
| `PaymentTerms` | `payment_terms` | string | ✅ | - | Termos de pagamento em dias |
| - | `invoice_currency` | string | ✅ | `literal('EUR')` | Moeda padrão EUR |
| - | `is_invoice` | boolean | ✅ | `literal('1')` | 1=invoice, 0=credit_memo |
| - | `reference_invoice_id` | string | ❌ | - | Para credit memos |
| - | `is_intercompany` | boolean | ✅ | `literal('0')` | 0=externo, 1=intercompany |

### Campos de Cliente (Customer Fields)

| Campo API (Source) | Campo NetSuite (Target) | Tipo | Obrigatório | Transformação | Notas |
|-------------------|------------------------|------|-------------|---------------|-------|
| `CustomerCode` | `customer_id` | string | ✅ | - | Identificador único do cliente |
| - | `first_name` | string | ❌ | - | Virá da tabela customer |
| - | `last_name` | string | ❌ | - | Virá da tabela customer |
| - | `company_name` | string | ❌ | - | Virá da tabela customer |
| - | `is_company` | boolean | ✅ | `literal('1')` | Default: empresa |
| - | `customer_currency` | string | ✅ | `literal('EUR')` | Moeda padrão do cliente |
| - | `tax_reg_number` | string | ❌ | - | Virá da tabela customer |
| - | `country` | string | ✅ | `literal('GR')` | Grécia (ISO 3166-1) |

### Campos de Linha (Invoice Line Items)

| Campo API (Source) | Campo NetSuite (Target) | Tipo | Obrigatório | Transformação | Notas |
|-------------------|------------------------|------|-------------|---------------|-------|
| `GID` | `invoice_line_id` | string | ✅ | - | ID único da linha |
| `ServiceCode` | `item_id` | string | ✅ | - | Código do serviço/item |
| `ServiceDescription` | `item_name` | string | ✅ | - | Descrição do serviço |
| - | `department` | string | ❌ | - | Departamento NetSuite (TBD) |
| `Quantity` | `quantity` | decimal(15,4) | ✅ | - | Quantidade |
| `Price` | `rate` | decimal(15,2) | ✅ | - | Preço unitário |
| `CurrencyNetValue` | `amount` | decimal(15,2) | ✅ | - | Valor total da linha |
| `CurrencyVATValue` | `tax_amount` | decimal(15,2) | ✅ | - | Valor do IVA/taxa |
| - | `contract_start_date` | date | ❌ | - | Para itens de assinatura |
| - | `contract_end_date` | date | ❌ | - | Para itens de assinatura |
| - | `rev_rec_start_date` | date | ❌ | - | Reconhecimento de receita |
| - | `rev_rec_end_date` | date | ❌ | - | Reconhecimento de receita |
| - | `is_recurring` | boolean | ✅ | `literal('0')` | Default: one-time |

## 📊 Campos Disponíveis na API (não mapeados)

Campos que vêm da API Entersoft mas **não são mapeados** no contrato atual:

- `GID` - Global identifier (usado apenas para invoice_line_id)
- `fADDocumentTypeGID` - Document type GID
- `fTradeAccountGID` - Trade account GID
- `fPaymentMethodGID` - Payment method GID
- `PaymentMethodCode` - Código do método de pagamento
- `Description` - Descrição do documento
- `CurrencyBaseValue` - Valor base na moeda
- `CurrencyTradeDiscountValue` - Desconto comercial
- `CurrencyTotalValue` - Valor total
- `DocTypeCode` / `DocTypeDescription` - Tipo de documento
- `DWHCode` / `DWHDescription` - Data warehouse
- `PaymentMethod` - Nome do método de pagamento
- `BillingCycle` - Ciclo de faturação
- `ADAlternativeCode` - Código alternativo (usado como Memo)
- `CustProfileCode` - Código do perfil do cliente (usado como Tax Code ID)
- `CustProfileDescription` - Descrição do perfil

## 🔗 Integração com Customer Table

Alguns campos não vêm diretamente da API de invoices e precisam ser enriquecidos com dados da tabela `customer`:

```sql
SELECT 
    i.*,
    c.first_name,
    c.last_name,
    c.company_name,
    c.tax_reg_number
FROM billing_seu_32h_invoices i
LEFT JOIN billing_seu_32h_customers c 
    ON i.customer_id = c.customer_id
```

## 🎯 Campos Calculados

Alguns campos precisam ser calculados ou inferidos:

1. **is_company**: Default `1` (empresa), mas pode ser refinado baseado em dados do cliente
2. **is_recurring**: Default `0` (one-time), mas pode ser calculado baseado em `ServiceCode` ou padrões
3. **department**: Precisa ser mapeado baseado em `ServiceCode` ou outra lógica de negócio
4. **contract_start_date / contract_end_date**: Para serviços de assinatura, calcular baseado em `BillingCycle`
5. **rev_rec_start_date / rev_rec_end_date**: Calcular baseado nas datas do contrato

## 📝 Exemplos

### Exemplo 1: Invoice Simples

**Dados da API:**
```json
{
  "ADCode": "INV_2025_001",
  "ADRegistrationDate": "2025-10-21",
  "CustomerCode": "CUST_123",
  "PaymentTerms": "30",
  "ServiceCode": "WEB_HOST_PREMIUM",
  "ServiceDescription": "Web Hosting Premium",
  "Quantity": 1,
  "Price": 49.99,
  "CurrencyNetValue": 49.99,
  "CurrencyVATValue": 11.98
}
```

**Resultado NetSuite:**
```json
{
  "invoice_id": "INV_2025_001",
  "invoice_number": "INV_2025_001",
  "entity": "32H",
  "platform": "Enartia",
  "brand": "Enartia",
  "legal_entity": "Enartia S.A.",
  "customer_id": "CUST_123",
  "invoice_date": "2025-10-21",
  "payment_terms": "30",
  "invoice_currency": "EUR",
  "is_invoice": "true",
  "country": "GR",
  "item_list": [{
    "item_id": "WEB_HOST_PREMIUM",
    "item_name": "Web Hosting Premium",
    "quantity": "1",
    "rate": "49.99",
    "amount": "49.99",
    "tax_amount": "11.98"
  }]
}
```

## ⚠️ Pontos de Atenção

1. **Moeda**: Todas as transações assumem EUR como padrão. Verificar se há casos com outras moedas.
2. **País**: Default é GR (Grécia), mas pode haver clientes de outros países.
3. **Tax Code**: O campo `CustProfileCode` da API é mapeado como `Tax Code ID` - verificar se está correto.
4. **Datas**: A API retorna datas em formato ISO (`yyyy-MM-dd`), que é o formato esperado pelo NetSuite.
5. **Recurring**: Atualmente todos os itens são marcados como `is_recurring: false`. Implementar lógica para identificar serviços recorrentes.

## 🔄 Próximos Passos

1. ✅ Implementar lógica para identificar `is_recurring` baseado em `ServiceCode` ou `BillingCycle`
2. ✅ Mapear códigos de departamento baseado em `ServiceCode`
3. ✅ Calcular datas de contrato e revenue recognition para itens recorrentes
4. ✅ Enriquecer com dados da tabela customer na camada Silver
5. ✅ Validar moedas alternativas se existirem
6. ✅ Testar integração completa RAW → Bronze → Silver → NetSuite

---

**Última atualização**: 2025-10-21  
**Entidade**: 32H (Enartia S.A.)  
**Região**: SEU (Southern Europe - Greece)

