#![allow(dead_code)]

use anyhow::{Result, anyhow};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    instruction::Instruction,
    message::Message,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    system_instruction,
    transaction::Transaction,
};

pub struct TxBuilderConfig {
    pub compute_units: u32,
    pub priority_fee_micro_lamports: u64, // micro-lamports per CU
}

impl Default for TxBuilderConfig {
    fn default() -> Self {
        Self {
            compute_units: 200_000,
            priority_fee_micro_lamports: 1_000, // 0.001 lamports/CU
        }
    }
}

pub struct TxBuilder {
    config: TxBuilderConfig,
}

impl TxBuilder {
    pub fn new(config: TxBuilderConfig) -> Self {
        Self { config }
    }

    /// Obtiene el blockhash más reciente
    pub async fn get_recent_blockhash(&self, rpc: &RpcClient) -> Result<Hash> {
        let blockhash = rpc
            .get_latest_blockhash()
            .await
            .map_err(|e| anyhow!("Failed to get blockhash: {}", e))?;
        Ok(blockhash)
    }

    /// Construye las instrucciones de ComputeBudget
    fn compute_budget_ixs(&self) -> Vec<Instruction> {
        vec![
            ComputeBudgetInstruction::set_compute_unit_limit(self.config.compute_units),
            ComputeBudgetInstruction::set_compute_unit_price(self.config.priority_fee_micro_lamports),
        ]
    }

    /// Construye una transacción completa con ComputeBudget + instrucciones custom
    pub fn build_transaction(
        &self,
        payer: &Keypair,
        instructions: Vec<Instruction>,
        recent_blockhash: Hash,
    ) -> Result<Transaction> {
        // Prepend compute budget instructions
        let mut all_ixs = self.compute_budget_ixs();
        all_ixs.extend(instructions);

        let message = Message::new(&all_ixs, Some(&payer.pubkey()));
        let mut tx = Transaction::new_unsigned(message);
        tx.sign(&[payer], recent_blockhash);

        Ok(tx)
    }

    /// Construye una transacción de prueba (solo ComputeBudget, sin swap real)
    pub fn build_test_transaction(
        &self,
        payer: &Keypair,
        recent_blockhash: Hash,
    ) -> Result<Transaction> {
        // Solo ComputeBudget (transacción "vacía" para testear pipeline)
        let ixs = self.compute_budget_ixs();

        let message = Message::new(&ixs, Some(&payer.pubkey()));
        let mut tx = Transaction::new_unsigned(message);
        tx.sign(&[payer], recent_blockhash);

        Ok(tx)
    }

    /// Estima el costo en lamports de la transacción
    pub fn estimate_fee(&self) -> u64 {
        // Base fee (5000 lamports) + priority fee
        let priority_fee = (self.config.compute_units as u64 * self.config.priority_fee_micro_lamports) / 1_000_000;
        5_000 + priority_fee
    }

    /// Construye una transacción de tip para Jito bundles
    /// El tip va a una de las tip accounts de Jito
    pub fn build_tip_tx(
        &self,
        payer: &Keypair,
        recent_blockhash: Hash,
        tip_account: &Pubkey,
        tip_lamports: u64,
    ) -> Result<Transaction> {
        // Compute budget (opcional pero no daña)
        let mut ixs = self.compute_budget_ixs();
        
        // Transfer del tip a la cuenta de Jito
        ixs.push(system_instruction::transfer(
            &payer.pubkey(),
            tip_account,
            tip_lamports,
        ));

        let message = Message::new(&ixs, Some(&payer.pubkey()));
        let mut tx = Transaction::new_unsigned(message);
        tx.sign(&[payer], recent_blockhash);
        
        Ok(tx)
    }

    /// Construye una transacción dummy (solo transfer pequeño) para test de bundles
    pub fn build_dummy_tx(
        &self,
        payer: &Keypair,
        recent_blockhash: Hash,
    ) -> Result<Transaction> {
        let mut ixs = self.compute_budget_ixs();
        
        // Self-transfer de 0 lamports (válido pero no hace nada)
        ixs.push(system_instruction::transfer(
            &payer.pubkey(),
            &payer.pubkey(),
            0,
        ));

        let message = Message::new(&ixs, Some(&payer.pubkey()));
        let mut tx = Transaction::new_unsigned(message);
        tx.sign(&[payer], recent_blockhash);
        
        Ok(tx)
    }
}

/// Helper para crear instrucciones de swap (placeholder)
pub struct SwapInstructionBuilder;

impl SwapInstructionBuilder {
    /// Placeholder: en producción esto construiría la instrucción real del DEX
    pub fn build_buy_instruction(
        _payer: &Pubkey,
        _mint: &Pubkey,
        _amount_in_lamports: u64,
    ) -> Vec<Instruction> {
        // TODO: Implementar según el DEX (Raydium, Jupiter, etc.)
        // Por ahora retornamos vacío
        println!("🔧 [TX_BUILDER] build_buy_instruction placeholder");
        vec![]
    }

    /// Placeholder: en producción esto construiría la instrucción real del DEX
    pub fn build_sell_instruction(
        _payer: &Pubkey,
        _mint: &Pubkey,
        _amount_tokens: u64,
    ) -> Vec<Instruction> {
        // TODO: Implementar según el DEX
        println!("🔧 [TX_BUILDER] build_sell_instruction placeholder");
        vec![]
    }
}
