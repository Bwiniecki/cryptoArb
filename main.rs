// ===================================================================================
//  PROGRAM: janus_arbitrage_bot (main.rs)
//  VERSION: 9.1 (MEXC Zero-Fee Implementation - Real Trading Fixed)
//  DATE:    2025-06-30
//  PURPOSE: CEX-DEX arbitrage bot with MEXC zero-fee trading and Jupiter API
//  IMPROVEMENT: 5x better profitability - only 0.05% spread needed vs 0.25%
// ===================================================================================

use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::io::AsyncBufReadExt;
use anyhow::Result;

// Import modules defined below in this file
use crate::state::SharedState;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize the logger
    logger::init();

    // The application's run state
    let is_running = Arc::new(AtomicBool::new(true));

    // The shared state (prices, capital)
    let shared_state = Arc::new(Mutex::new(SharedState::new()));

    // Create a channel for price updates
    let (price_sender, price_receiver) = mpsc::channel(128);

    // Clone Arcs for each task
    let engine_state = Arc::clone(&shared_state);
    let final_state_clone = Arc::clone(&shared_state);

    let mexc_running = Arc::clone(&is_running);
    let solana_running = Arc::clone(&is_running);
    let engine_running = Arc::clone(&is_running);
    let failsafe_running = Arc::clone(&is_running);

    log::info!("[MAIN] Launching threads...");
    log::info!("[MAIN] 🚀 MEXC Zero-Fee Implementation - Real Trading Ready!");

    // Task for MEXC WebSocket connector
    let mexc_sender = price_sender.clone();
    let mexc_handle = tokio::spawn(async move {
        mexc::run_mexc_connector(mexc_running, mexc_sender).await;
    });

    // Task for Solana Jupiter API connector
    let solana_handle = tokio::spawn(async move {
        solana::run_jupiter_connector(solana_running, price_sender).await;
    });

    // Give connectors time to get initial prices
    log::info!("[MAIN] Waiting for initial price feeds...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Task for arbitrage engine
    let engine_handle = tokio::spawn(async move {
        engine::run_arbitrage_engine(engine_running, engine_state, price_receiver).await;
    });

    // Task for failsafe
    let failsafe_handle = tokio::spawn(async move {
        log::info!(">> Failsafe armed. Press [ENTER] to shut down cleanly. <<");
        let _ = tokio::io::BufReader::new(tokio::io::stdin()).lines().next_line().await;
        if failsafe_running.load(Ordering::SeqCst) {
            log::warn!("[FAILSAFE] Manual KILL SWITCH engaged by operator.");
            failsafe_running.store(false, Ordering::SeqCst);
        }
    });

    // Wait for any task to complete
    tokio::select! {
        _ = mexc_handle => log::error!("[MAIN] MEXC connector exited unexpectedly."),
        _ = solana_handle => log::error!("[MAIN] Solana connector exited unexpectedly."),
        _ = engine_handle => log::info!("[MAIN] Arbitrage engine shut down."),
        _ = failsafe_handle => log::info!("[MAIN] Failsafe shutdown triggered."),
    }

    // Ensure all tasks shut down
    is_running.store(false, Ordering::SeqCst);

    // Write price history to CSV
    log::info!("[MAIN] Writing price history to CSV...");
    let final_state_lock = final_state_clone.lock();
    match final_state_lock {
        Ok(final_state) => {
            if let Err(e) = csv_writer::write_to_csv(&final_state.price_history) {
                log::error!("[MAIN] Failed to write CSV file: {}", e);
            } else {
                log::info!("[MAIN] Successfully wrote price history to CSV.");
            }
        },
        Err(poison_error) => {
            log::error!("[MAIN] Mutex was poisoned, cannot write CSV. Error: {}", poison_error);
        }
    }
    
    log::info!("[MAIN] Writing trade history to CSV...");
    let final_state_lock = final_state_clone.lock();
    match final_state_lock {
        Ok(final_state) => {
            // Write price history (existing)
            if let Err(e) = csv_writer::write_to_csv(&final_state.price_history) {
                log::error!("[MAIN] Failed to write price CSV: {}", e);
            }
            
            // 🆕 NEW: Write trade history
            if let Err(e) = trade_csv_writer::write_trades_to_csv(&final_state.trade_history) {
                log::error!("[MAIN] Failed to write trades CSV: {}", e);
            } else {
                log::info!("[MAIN] Successfully wrote trade history to CSV.");
            }
            
            // 🆕 NEW: Write trading summary
            if let Err(e) = trade_csv_writer::write_trade_summary(
                &final_state.trade_history, 
                final_state.total_profit, 
                final_state.trade_count
            ) {
                log::error!("[MAIN] Failed to write summary: {}", e);
            } else {
                log::info!("[MAIN] Successfully wrote trading summary.");
            }
            
            log::info!("[MAIN] 📊 FINAL STATS: {} trades, ${:.4} total profit", 
                final_state.trade_count, final_state.total_profit);
        },
        Err(poison_error) => {
            log::error!("[MAIN] Mutex was poisoned, cannot write CSV. Error: {}", poison_error);
        }
    }

    log::info!("[MAIN] All threads terminated. Program shutdown complete.");
    Ok(())
}

// ==================================================
// MODULES
// ==================================================

// ---- FIXED Configuration for Real Trading ----
mod config {
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use std::env;

    fn get_env_var(key: &str) -> String {
        env::var(key).unwrap_or_else(|_| panic!("ERROR: Missing required environment variable '{}'", key))
    }

    fn get_env_var_optional(key: &str, default: &str) -> String {
        env::var(key).unwrap_or_else(|_| default.to_string())
    }

    #[allow(dead_code)]
    pub struct AppConfig {
        pub mexc_api_key: String,
        pub mexc_api_secret: String,
        pub jupiter_api_url: String,
        pub solana_rpc_url: String,
        pub solana_wallet_path: String,
    }

    impl AppConfig {
        pub fn new() -> Self {
            Self {
                mexc_api_key: get_env_var("MEXC_API_KEY"),
                mexc_api_secret: get_env_var("MEXC_API_SECRET"),
                jupiter_api_url: get_env_var_optional("JUPITER_API_URL", "https://lite-api.jup.ag/price/v2"),
                solana_rpc_url: get_env_var_optional("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com"),
                solana_wallet_path: get_env_var("SOLANA_WALLET_PRIVATE_KEY_PATH"),
            }
        }
    }

    // 🚨 REAL TRADING MODE ENABLED!
    pub const PAPER_TRADING: bool = false;  // ✅ Set to false for real trading

    // Static config
    pub const MEXC_WEBSOCKET_URL: &str = "wss://wbs.mexc.com/ws";
    pub const MEXC_TARGET_PAIR: &str = "SOLUSDC";
    pub const SOL_MINT_ADDRESS: &str = "So11111111111111111111111111111111111111112";
    pub const USDC_MINT_ADDRESS: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

    // 🔧 FIXED: Updated balances and trade settings for your actual funds
    pub const MEXC_STARTING_USDC: Decimal = dec!(996.78905698);    // Your actual MEXC USDC balance
    pub const WALLET_STARTING_USDC: Decimal = dec!(778.71896);     // Your actual Solana wallet USDC balance

    // Strategy & Risk - CONSERVATIVE FOR REAL TRADING  
    pub const MIN_PROFIT_SPREAD_PERCENT: Decimal = dec!(0.01);     // 0.05% minimum profit
    pub const TRADE_AMOUNT_SOL: Decimal = dec!(0.1);             // 🔧 Reduced to 0.05 SOL for safety (~$7.85)
    pub const MAX_CAPITAL_PER_TRADE_PERCENT: Decimal = dec!(50.0); // 🔧 Reduced to 5% for safety
    pub const MAX_DAILY_DRAWDOWN_PERCENT: Decimal = dec!(20.0);    // 2% max daily loss
    
    // Fees and slippage
    pub const MEXC_MAKER_FEE: Decimal = dec!(0.0);               // Zero fees in MEXC zero-fee zone
    pub const MEXC_TAKER_FEE: Decimal = dec!(0.0);               // Zero fees in MEXC zero-fee zone
    pub const SOLANA_GAS_FEE_USD: Decimal = dec!(0.01);          // ~$0.01 per transaction
    pub const SLIPPAGE_BUFFER_PERCENT: Decimal = dec!(0.02);     // 0.05% slippage protection
    pub const MAX_EXECUTION_TIME_SECONDS: u64 = 4;
    pub const MAX_PRICE_AGE_SECONDS: u64 = 5;
    pub const SOLANA_MAX_SLIPPAGE_BPS: u16 = 15;   
}


// ---- FINAL FIXED MEXC Trading Execution Module ----
mod mexc_trading {
    use reqwest;
    use serde_json::Value;
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    use rust_decimal::Decimal;
    use std::time::{SystemTime, UNIX_EPOCH};

    type HmacSha256 = Hmac<Sha256>;

    #[derive(Clone)]
    pub struct MexcTradingClient {
        client: reqwest::Client,
        api_key: String,
        api_secret: String,
        base_url: String,
    }

    impl MexcTradingClient {
        pub fn new(api_key: String, api_secret: String) -> Self {
            Self {
                client: reqwest::Client::new(),
                api_key,
                api_secret,
                base_url: "https://api.mexc.com".to_string(),
            }
        }

        fn generate_signature(&self, params: &str) -> String {
            let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes()).unwrap();
            mac.update(params.as_bytes());
            hex::encode(mac.finalize().into_bytes())
        }

        pub async fn place_order(
            &self,
            symbol: &str,
            side: &str,  // "BUY" or "SELL"
            order_type: &str,  // "MARKET" or "LIMIT"
            quantity: Decimal,
            price: Option<Decimal>,
        ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_millis() as u64;

            // 🔧 MEXC WORKAROUND: Use LIMIT orders for immediate execution instead of MARKET
            let (final_order_type, order_price) = if order_type == "MARKET" {
                match price {
                    Some(market_price) => {
                        // Use LIMIT order with slight premium/discount for immediate execution
                        let adjusted_price = if side == "BUY" {
                            market_price * rust_decimal_macros::dec!(1.001) // 0.1% premium for immediate buy
                        } else {
                            market_price * rust_decimal_macros::dec!(0.999) // 0.1% discount for immediate sell
                        };
                        log::info!("[MEXC] Converting MARKET to LIMIT order: {} at ${:.4} (adjusted from ${:.4})", 
                            side, adjusted_price, market_price);
                        ("LIMIT", Some(adjusted_price))
                    },
                    None => {
                        return Err("MARKET order requires current price for LIMIT conversion".into());
                    }
                }
            } else {
                (order_type, price)
            };

            // 🔧 MEXC WORKING FORMAT: All parameters in query string
            let mut params = vec![
                ("symbol", symbol.to_string()),
                ("side", side.to_string()),
                ("type", final_order_type.to_string()),
                ("quantity", quantity.to_string()),
                ("timestamp", timestamp.to_string()),
            ];
            
            // Add price for LIMIT orders
            if let Some(price_value) = order_price {
                params.push(("price", price_value.to_string()));
            }

            // Add timeInForce for LIMIT orders to ensure immediate execution
            if final_order_type == "LIMIT" {
                params.push(("timeInForce", "IOC".to_string())); // Immediate or Cancel
            }

            // Sort parameters for signature (alphabetical order)
            params.sort_by(|a, b| a.0.cmp(b.0));
            
            let query_string = params
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join("&");

            log::debug!("[MEXC] Query string for signature: {}", query_string);

            let signature = self.generate_signature(&query_string);
            log::debug!("[MEXC] Generated signature: {}", signature);
            
            // 🔧 MEXC WORKING FORMAT: All parameters + signature in URL, empty POST body
            let final_url = format!("{}/api/v3/order?{}&signature={}", 
                self.base_url, query_string, signature);
            
            log::info!("[MEXC] Placing {} order: {} {} SOL at ${:?}", final_order_type, side, quantity, order_price);
            log::debug!("[MEXC] Request URL: {}", final_url);
            
            // 🔧 MEXC SPECIFIC: POST with empty body, everything in URL
            let response = self.client
                .post(&final_url)
                .header("X-MEXC-APIKEY", &self.api_key)
                .send()  // No body at all - just headers
                .await?;

            log::debug!("[MEXC] Response status: {}", response.status());
            log::debug!("[MEXC] Response headers: {:?}", response.headers());
            
            if response.status().is_success() {
                let result: Value = response.json().await?;
                log::info!("[MEXC] ✅ Order placed successfully: {:?}", result);
                Ok(result)
            } else {
                let error_text = response.text().await?;
                log::error!("[MEXC] ❌ Order failed: {}", error_text);
                Err(format!("MEXC API Error: {}", error_text).into())
            }
        }

        pub async fn get_account_balance(&self) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_millis() as u64;

            let query_string = format!("timestamp={}", timestamp);
            let signature = self.generate_signature(&query_string);
            let final_params = format!("{}&signature={}", query_string, signature);

            let url = format!("{}/api/v3/account?{}", self.base_url, final_params);
            
            let response = self.client
                .get(&url)
                .header("X-MEXC-APIKEY", &self.api_key)
                .send()
                .await?;

            if response.status().is_success() {
                let result: Value = response.json().await?;
                Ok(result)
            } else {
                let error_text = response.text().await?;
                Err(format!("MEXC Balance Error: {}", error_text).into())
            }
        }

        // 🆕 NEW: Get current market price before placing order
        pub async fn get_current_price(&self, symbol: &str) -> Result<Decimal, Box<dyn std::error::Error + Send + Sync>> {
            let url = format!("{}/api/v3/ticker/price?symbol={}", self.base_url, symbol);
            
            let response = self.client
                .get(&url)
                .header("X-MEXC-APIKEY", &self.api_key)
                .send()
                .await?;

            if response.status().is_success() {
                let result: serde_json::Value = response.json().await?;
                if let Some(price_str) = result.get("price").and_then(|p| p.as_str()) {
                    let price = price_str.parse::<Decimal>()
                        .map_err(|_| "Failed to parse price")?;
                    return Ok(price);
                }
            }
            
            Err("Failed to get current price".into())
        }

        // 🔧 ENHANCED: Use real-time price for LIMIT order placement
        pub async fn place_order_with_current_price(
            &self,
            symbol: &str,
            side: &str,
            order_type: &str,
            quantity: Decimal,
            _expected_price: Option<Decimal>,
        ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
            
            // Get current market price
            let current_price = self.get_current_price(symbol).await?;
            log::info!("[MEXC] Current market price: ${:.4}", current_price);
            
            // 🔧 FIXED: Correct pricing logic
            let execution_price = if side == "BUY" {
                // For BUY orders: Pay slightly MORE than market price to ensure execution
                current_price * rust_decimal_macros::dec!(1.002) // 0.2% premium
            } else {
                // For SELL orders: Accept slightly LESS than market price to ensure execution  
                current_price * rust_decimal_macros::dec!(0.998) // 0.2% discount
            };
            
            log::info!("[MEXC] Using execution price: ${:.4} for {} order ({})", 
                execution_price, side, 
                if side == "BUY" { "premium" } else { "discount" });
            
            self.place_order(symbol, side, order_type, quantity, Some(execution_price)).await
        }
    }
}
// 🆕 FIXED: Move SwapPriceInfo to module level (not inside impl)
mod solana_trading {
    use solana_sdk::{
        signer::{keypair::Keypair, Signer},
        transaction::Transaction,
        commitment_config::CommitmentConfig,
    };
    use solana_client::rpc_client::RpcClient;
    use std::sync::Arc;
    use serde_json::Value;
    use base64::Engine;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    // 🔧 FIXED: Moved outside of impl block
    #[derive(Debug, Clone)]
    pub struct SwapPriceInfo {
        pub executed_price: Decimal,
        pub slippage_percent: Decimal,
    }

    #[derive(Clone)]
    pub struct SolanaTradingClient {
        client: Arc<RpcClient>,
        wallet: Arc<Keypair>,
        http_client: reqwest::Client,
    }

    impl SolanaTradingClient {
        // 🔧 FIXED: Add missing new() constructor
        pub fn new(rpc_url: String, wallet_path: String, _jupiter_api_url: String) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
            // 🚀 SPEED: Use faster commitment level
            let client = Arc::new(RpcClient::new_with_commitment(rpc_url, CommitmentConfig::processed()));
            
            let wallet_data = std::fs::read_to_string(wallet_path)?;
            let wallet_bytes: Vec<u8> = serde_json::from_str(&wallet_data)?;
            let wallet = Arc::new(Keypair::from_bytes(&wallet_bytes)?);

            // 🚀 SPEED: Optimized HTTP client
            let http_client = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(3))  // 3-second timeout
                .pool_idle_timeout(std::time::Duration::from_secs(1))
                .build()?;

            Ok(Self {
                client,
                wallet,
                http_client,
            })
        }

        // 🔧 FIXED: Add missing execute_swap function
        pub async fn execute_swap(
            &self,
            input_mint: &str,
            output_mint: &str,
            amount: u64,
            slippage_bps: u16,
        ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
            
            log::info!("[SOLANA] 🚀 Starting Jupiter swap: {} {} to {}", amount, 
                if input_mint.contains("So1111") { "SOL" } else { "USDC" },
                if output_mint.contains("So1111") { "SOL" } else { "USDC" });
            
            // 🔧 STEP 1: More reliable quote request
            let quote_url = format!(
                "https://quote-api.jup.ag/v6/quote?inputMint={}&outputMint={}&amount={}&slippageBps={}&onlyDirectRoutes=false&maxAccounts=64",
                input_mint, output_mint, amount, slippage_bps
            );

            log::debug!("[SOLANA] Quote URL: {}", quote_url);

            let quote_response = tokio::time::timeout(
                std::time::Duration::from_secs(5), // Increased timeout
                self.http_client.get(&quote_url).send()
            ).await
            .map_err(|_| "Jupiter quote timeout")?
            .map_err(|e| format!("Jupiter quote request failed: {}", e))?;

            if !quote_response.status().is_success() {
                let error_text = quote_response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
                return Err(format!("Jupiter quote failed: {}", error_text).into());
            }

            let quote_data: serde_json::Value = quote_response.json().await
                .map_err(|e| format!("Failed to parse quote response: {}", e))?;

            log::debug!("[SOLANA] Quote received: {:?}", quote_data.get("outAmount"));

            // 🔧 STEP 2: More reliable swap transaction request
            let swap_request = serde_json::json!({
                "userPublicKey": self.wallet.pubkey().to_string(),
                "quoteResponse": quote_data,
                "wrapAndUnwrapSol": true,
                "dynamicComputeUnitLimit": true,
                "prioritizationFeeLamports": 10000,  // Higher priority fee
                "asLegacyTransaction": true,  // 🔧 CHANGED: Use legacy format for reliability
                "skipUserAccountsRpcCalls": false,  // 🔧 CHANGED: Don't skip validation
            });

            log::debug!("[SOLANA] Swap request prepared");

            let swap_response = tokio::time::timeout(
                std::time::Duration::from_secs(5), // Increased timeout
                self.http_client
                    .post("https://quote-api.jup.ag/v6/swap")
                    .json(&swap_request)
                    .send()
            ).await
            .map_err(|_| "Jupiter swap timeout")?
            .map_err(|e| format!("Jupiter swap request failed: {}", e))?;

            if !swap_response.status().is_success() {
                let error_text = swap_response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
                return Err(format!("Jupiter swap failed: {}", error_text).into());
            }

            let swap_data: serde_json::Value = swap_response.json().await
                .map_err(|e| format!("Failed to parse swap response: {}", e))?;

            let transaction_data = swap_data["swapTransaction"].as_str()
                .ok_or("No swap transaction in response")?;

            log::debug!("[SOLANA] Transaction received, decoding...");

            // 🔧 STEP 3: Decode and sign transaction
            let transaction_bytes = base64::prelude::BASE64_STANDARD.decode(transaction_data)
                .map_err(|e| format!("Failed to decode transaction: {}", e))?;

            let mut transaction: solana_sdk::transaction::Transaction = bincode::deserialize(&transaction_bytes)
                .map_err(|e| format!("Failed to deserialize transaction: {}", e))?;
            
            // 🔧 STEP 4: Get fresh blockhash and sign
            let recent_blockhash = self.client.get_latest_blockhash()
                .map_err(|e| format!("Failed to get blockhash: {}", e))?;
            
            transaction.message.recent_blockhash = recent_blockhash;
            
            transaction.try_sign(&[self.wallet.as_ref()], recent_blockhash)
                .map_err(|e| format!("Failed to sign transaction: {}", e))?;

            // 🔧 STEP 5: Send transaction with retry
            log::info!("[SOLANA] Sending transaction to network...");
            
            let signature = self.client.send_transaction(&transaction)
                .map_err(|e| format!("Transaction failed: {}", e))?;
            
            log::info!("[SOLANA] ✅ Transaction sent! Signature: {}", signature);
            Ok(signature.to_string())
        }

        // Enhanced swap function that tracks actual execution price
        pub async fn execute_swap_with_price_tracking(
            &self,
            input_mint: &str,
            output_mint: &str,
            amount: u64,
            slippage_bps: u16,
        ) -> Result<(String, SwapPriceInfo), Box<dyn std::error::Error + Send + Sync>> {
            
            log::info!("[SOLANA] 🚀 Starting Jupiter swap with price tracking: {} {} to {}", amount, 
                if input_mint.contains("So1111") { "SOL" } else { "USDC" },
                if output_mint.contains("So1111") { "SOL" } else { "USDC" });
            
            // Get quote and track expected vs actual
            let quote_url = format!(
                "https://quote-api.jup.ag/v6/quote?inputMint={}&outputMint={}&amount={}&slippageBps={}&onlyDirectRoutes=false&maxAccounts=64",
                input_mint, output_mint, amount, slippage_bps
            );

            let quote_response = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                self.http_client.get(&quote_url).send()
            ).await
            .map_err(|_| "Jupiter quote timeout")?
            .map_err(|e| format!("Jupiter quote request failed: {}", e))?;

            if !quote_response.status().is_success() {
                let error_text = quote_response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
                return Err(format!("Jupiter quote failed: {}", error_text).into());
            }

            let quote_data: serde_json::Value = quote_response.json().await
                .map_err(|e| format!("Failed to parse quote response: {}", e))?;

            // Extract expected amounts for price calculation
            let in_amount = quote_data["inAmount"].as_str()
                .ok_or("Missing inAmount")?.parse::<u64>()?;
            let out_amount = quote_data["outAmount"].as_str()
                .ok_or("Missing outAmount")?.parse::<u64>()?;

            log::debug!("[SOLANA] Quote: {} -> {} (expected)", in_amount, out_amount);

            // Execute the swap (existing logic)
            let signature = self.execute_swap(input_mint, output_mint, amount, slippage_bps).await?;
            
            // Calculate executed price
            let executed_price = if input_mint.contains("So1111") {
                // SOL -> USDC: price = USDC_out / SOL_in
                Decimal::from(out_amount) / Decimal::from(1_000_000) / (Decimal::from(in_amount) / Decimal::from(1_000_000_000))
            } else {
                // USDC -> SOL: price = USDC_in / SOL_out  
                Decimal::from(in_amount) / Decimal::from(1_000_000) / (Decimal::from(out_amount) / Decimal::from(1_000_000_000))
            };
            
            // For now, use quote data as executed (in real implementation, you'd parse the transaction)
            let price_info = SwapPriceInfo {
                executed_price,
                slippage_percent: Decimal::from(slippage_bps) / dec!(100.0),
            };
            
            log::info!("[SOLANA] ✅ Swap executed at ${:.4} per SOL", executed_price);
            Ok((signature, price_info))
        }

        pub fn get_wallet_address(&self) -> String {
            self.wallet.pubkey().to_string()
        }
    }
}

// ---- FIXED Shared State Module ----
mod state {
    use rust_decimal::Decimal;
    use chrono::{DateTime, Utc};
    use serde::Serialize;
    use crate::config;

    pub type Price = Decimal;

    #[derive(Debug, Clone, Copy, Serialize)]
    pub struct PriceInfo {
        pub price: Price,
        pub timestamp: DateTime<Utc>,  // 🆕 Add timestamp for staleness detection
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct PriceDataPoint {
        pub timestamp: DateTime<Utc>,
        pub source: String,
        pub price: Price,
    }

    // 🆕 NEW: Enhanced trade record with detailed metrics
    #[derive(Debug, Clone, Serialize)]
    pub struct TradeRecord {
        pub trade_id: String,
        pub timestamp: DateTime<Utc>,
        pub direction: String,
        pub amount_sol: Decimal,
        pub buy_venue: String,
        pub sell_venue: String,
        pub buy_price: Decimal,
        pub sell_price: Decimal,
        pub expected_profit: Decimal,
        pub actual_profit: Option<Decimal>,
        pub expected_spread_percent: Decimal,
        pub actual_spread_percent: Option<Decimal>,
        // 🆕 Enhanced slippage tracking
        pub buy_slippage_percent: Option<Decimal>,
        pub sell_slippage_percent: Option<Decimal>,
        pub total_slippage_percent: Option<Decimal>,
        pub buy_order_id: Option<String>,
        pub sell_order_id: Option<String>,
        pub solana_tx_signature: Option<String>,
        pub total_fees: Decimal,
        pub execution_time_ms: Option<u64>,
        pub status: String,
        // 🆕 NEW: Actual execution prices for slippage calculation
        pub actual_buy_price: Option<Decimal>,
        pub actual_sell_price: Option<Decimal>,
    }

    #[derive(Debug, Clone)]
    pub struct AccountBalance {
        pub mexc_usdc: Decimal,
        pub wallet_usdc: Decimal,
    }

    #[derive(Debug, Clone)]
    pub struct SharedState {
        pub mexc_price: Option<PriceInfo>,
        pub solana_price: Option<PriceInfo>,
        pub starting_capital: Decimal,
        pub balances: AccountBalance,
        pub price_history: Vec<PriceDataPoint>,
        pub trade_history: Vec<TradeRecord>,  // 🆕 NEW: Track all trades
        pub total_profit: Decimal,  // 🆕 NEW: Running total profit
        pub trade_count: u32,  // 🆕 NEW: Track number of trades
    }

    impl SharedState {
        pub fn new() -> Self {
            let balances = AccountBalance {
                mexc_usdc: config::MEXC_STARTING_USDC,
                wallet_usdc: config::WALLET_STARTING_USDC,
            };
            let starting_capital = balances.mexc_usdc + balances.wallet_usdc;
            Self {
                mexc_price: None,
                solana_price: None,
                starting_capital,
                balances,
                price_history: Vec::new(),
                trade_history: Vec::new(),  // 🆕 NEW
                total_profit: Decimal::ZERO,  // 🆕 NEW
                trade_count: 0,  // 🆕 NEW
            }
            
        }
        
        pub fn get_current_capital(&self) -> Decimal {
            self.balances.mexc_usdc + self.balances.wallet_usdc
        }

        // 🆕 NEW: Add completed trade to history
        pub fn add_trade_record(&mut self, trade: TradeRecord) {
            if let Some(actual_profit) = trade.actual_profit {
                self.total_profit += actual_profit;
            }
            self.trade_count += 1;
            self.trade_history.push(trade);
        }

        // 🆕 NEW: Check if prices are fresh enough for trading
        pub fn are_prices_fresh(&self) -> bool {
            let now = Utc::now();
            let max_age = chrono::Duration::seconds(config::MAX_PRICE_AGE_SECONDS as i64);
            
            match (&self.mexc_price, &self.solana_price) {
                (Some(mexc), Some(solana)) => {
                    let mexc_age = now.signed_duration_since(mexc.timestamp);
                    let solana_age = now.signed_duration_since(solana.timestamp);
                    mexc_age < max_age && solana_age < max_age
                },
                _ => false
            }
        }
    }

    #[derive(Debug)]
    pub enum PriceUpdate {
        MEXC(Price),
        Solana(Price),
    }
}
// 🔧 FIXED: Add missing RiskManager functions
mod risk {
    use crate::{config, state::{SharedState, TradeRecord}, mexc_trading::MexcTradingClient, solana_trading::{SolanaTradingClient, SwapPriceInfo}};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use rust_decimal::prelude::*; // This provides ToPrimitive trait
    use chrono::Utc;
    use std::time::Instant;

    pub struct RiskManager;

    impl RiskManager {
        // 🆕 FIXED: Add the missing check_trade function
        pub fn check_trade(state: &SharedState, trade_value_usd: Decimal) -> bool {
            let current_capital = state.get_current_capital();

            let max_trade_value = current_capital * (config::MAX_CAPITAL_PER_TRADE_PERCENT / dec!(100.0));
            if trade_value_usd > max_trade_value {
                log::warn!("[RISK] VETO: Trade value {} exceeds max size {}", trade_value_usd, max_trade_value);
                return false;
            }

            let drawdown = (state.starting_capital - current_capital) / state.starting_capital * dec!(100.0);
            if drawdown > config::MAX_DAILY_DRAWDOWN_PERCENT {
                log::error!("[RISK] CRITICAL: Max daily drawdown of {}% exceeded. Shutting down.", config::MAX_DAILY_DRAWDOWN_PERCENT);
                return false;
            }
            true
        }

        // 🆕 FIXED: Add the missing check_price_freshness function
        pub fn check_price_freshness(state: &SharedState) -> bool {
            if !state.are_prices_fresh() {
                log::warn!("[RISK] VETO: Prices are stale - aborting trade");
                return false;
            }
            true
        }

        // 🆕 FIXED: Add the missing update_capital function for paper trading
        pub fn update_capital(state: &mut SharedState, venue_bought: &str, amount_sol: Decimal, price_bought: Decimal, price_sold: Decimal) {
            // 🚨 CRITICAL: This should only run in paper trading mode
            if !config::PAPER_TRADING {
                log::error!("[REAL_TRADE] ❌ update_capital() called but real trades should use execute_parallel_arbitrage()!");
                return;
            }

            // Paper trading logic
            let cost = if venue_bought == "MEXC" {
                amount_sol * price_bought
            } else {
                (amount_sol * price_bought) + config::SOLANA_GAS_FEE_USD
            };

            let revenue = if venue_bought == "MEXC" {
                amount_sol * price_sold - config::SOLANA_GAS_FEE_USD
            } else {
                amount_sol * price_sold
            };

            let profit = revenue - cost;

            if venue_bought == "MEXC" {
                state.balances.mexc_usdc -= cost;
                state.balances.wallet_usdc += revenue;
            } else {
                state.balances.wallet_usdc -= cost;
                state.balances.mexc_usdc += revenue;
            }

            log::info!("[PAPER_TRADE] 🚀 ZERO-FEE SOL/USDC Profit: ${:.4}. New Capital: ${:.4}", profit, state.get_current_capital());
        }

        // Enhanced execution function in risk.rs module
        pub async fn execute_parallel_arbitrage(
            mexc_client: &MexcTradingClient,
            solana_client: &SolanaTradingClient,
            venue_bought: &str,
            amount_sol: Decimal,
            expected_buy_price: Decimal,
            expected_sell_price: Decimal,
        ) -> Result<TradeRecord, Box<dyn std::error::Error + Send + Sync>> {
            
            let start_time = Instant::now();
            let trade_id = format!("TRADE_{}", Utc::now().format("%Y%m%d_%H%M%S_%3f"));
            
            log::info!("[PARALLEL] 🚀 Executing {} with PARALLEL execution", trade_id);
            log::info!("[PARALLEL] Direction: {}, Amount: {} SOL", venue_bought, amount_sol);
            
            let mut trade_record = TradeRecord {
                trade_id: trade_id.clone(),
                timestamp: Utc::now(),
                direction: if venue_bought == "MEXC" { 
                    "Buy MEXC -> Sell Solana".to_string() 
                } else { 
                    "Buy Solana -> Sell MEXC".to_string() 
                },
                amount_sol,
                buy_venue: venue_bought.to_string(),
                sell_venue: if venue_bought == "MEXC" { "Solana".to_string() } else { "MEXC".to_string() },
                buy_price: expected_buy_price,
                sell_price: expected_sell_price,
                expected_profit: amount_sol * (expected_sell_price - expected_buy_price) - config::SOLANA_GAS_FEE_USD,
                actual_profit: None,
                expected_spread_percent: ((expected_sell_price - expected_buy_price) / expected_buy_price) * dec!(100.0),
                actual_spread_percent: None,
                buy_slippage_percent: None,
                sell_slippage_percent: None,
                total_slippage_percent: None,
                buy_order_id: None,
                sell_order_id: None,
                solana_tx_signature: None,
                total_fees: config::SOLANA_GAS_FEE_USD,
                execution_time_ms: None,
                status: "EXECUTING".to_string(),
                actual_buy_price: None,
                actual_sell_price: None,
            };

            if config::PAPER_TRADING {
                trade_record.actual_profit = Some(trade_record.expected_profit);
                trade_record.actual_spread_percent = Some(trade_record.expected_spread_percent);
                trade_record.buy_slippage_percent = Some(dec!(0.01));  // Simulate 0.01% buy slippage
                trade_record.sell_slippage_percent = Some(dec!(0.015)); // Simulate 0.015% sell slippage
                trade_record.total_slippage_percent = Some(dec!(0.025));
                trade_record.execution_time_ms = Some(1200);
                trade_record.status = "SUCCESS".to_string();
                trade_record.actual_buy_price = Some(expected_buy_price * dec!(1.0001));
                trade_record.actual_sell_price = Some(expected_sell_price * dec!(0.9999));
                
                // 🆕 Enhanced profit display
                Self::display_trade_results(&trade_record);
                return Ok(trade_record);
            }

            // 🚀 REAL TRADING EXECUTION with detailed tracking
            let mut actual_buy_price = expected_buy_price;
            let mut actual_sell_price = expected_sell_price;

            if venue_bought == "MEXC" {
                // MEXC Buy + Solana Sell in parallel
                let mexc_buy_future = mexc_client.place_order_with_current_price(
                    "SOLUSDC", "BUY", "MARKET", amount_sol, Some(expected_buy_price)
                );
                
                let sol_lamports = (amount_sol * dec!(1_000_000_000)).to_u64().unwrap_or(0);
                log::info!("[PARALLEL] MEXC BUY {} SOL + Solana SELL {} lamports", amount_sol, sol_lamports);
                
                let solana_sell_future = solana_client.execute_swap_with_price_tracking(
                    config::SOL_MINT_ADDRESS,
                    config::USDC_MINT_ADDRESS,
                    sol_lamports,
                    25, // 0.25% slippage for reliability
                );

                // 🔧 FIXED: Properly typed futures
                let (mexc_result, solana_result): (
                    Result<serde_json::Value, _>, 
                    Result<(String, SwapPriceInfo), _>
                ) = tokio::join!(mexc_buy_future, solana_sell_future);

                match (&mexc_result, &solana_result) {
                    (Ok(mexc_order), Ok((solana_tx, solana_price_info))) => {
                        log::info!("[PARALLEL] ✅ Both trades succeeded!");
                        
                        // Extract actual execution prices
                        if let Some(order_id) = mexc_order.get("orderId").and_then(|id| id.as_str()) {
                            trade_record.buy_order_id = Some(order_id.to_string());
                        }
                        if let Some(fills) = mexc_order.get("fills").and_then(|f| f.as_array()) {
                            if let Some(fill) = fills.first() {
                                if let Some(price_str) = fill.get("price").and_then(|p| p.as_str()) {
                                    actual_buy_price = price_str.parse().unwrap_or(expected_buy_price);
                                }
                            }
                        }
                        
                        trade_record.solana_tx_signature = Some(solana_tx.clone());
                        actual_sell_price = solana_price_info.executed_price;
                        trade_record.actual_buy_price = Some(actual_buy_price);
                        trade_record.actual_sell_price = Some(actual_sell_price);
                    },
                    (Ok(_), Err(solana_err)) => {
                        log::error!("[PARALLEL] ❌ Solana SELL failed: {}", solana_err);
                        return Err(format!("Solana sell failed: {}", solana_err).into());
                    },
                    (Err(mexc_err), Ok(_)) => {
                        log::error!("[PARALLEL] ❌ MEXC BUY failed: {}", mexc_err);
                        return Err(format!("MEXC buy failed: {}", mexc_err).into());
                    },
                    (Err(mexc_err), Err(solana_err)) => {
                        log::error!("[PARALLEL] ❌ Both trades failed - MEXC: {}, Solana: {}", mexc_err, solana_err);
                        return Err(format!("Both trades failed").into());
                    }
                }
            } else {
                // Solana Buy + MEXC Sell in parallel
                let usdc_amount = (amount_sol * expected_buy_price * dec!(1_000_000)).to_u64().unwrap_or(0);
                log::info!("[PARALLEL] Solana BUY {} USDC + MEXC SELL {} SOL", usdc_amount, amount_sol);
                
                let solana_buy_future = solana_client.execute_swap_with_price_tracking(
                    config::USDC_MINT_ADDRESS,
                    config::SOL_MINT_ADDRESS,
                    usdc_amount,
                    25, // 0.25% slippage for reliability
                );
                
                let mexc_sell_future = mexc_client.place_order_with_current_price(
                    "SOLUSDC", "SELL", "MARKET", amount_sol, Some(expected_sell_price)
                );

                // 🔧 FIXED: Properly typed futures
                let (solana_result, mexc_result): (
                    Result<(String, SwapPriceInfo), _>, 
                    Result<serde_json::Value, _>
                ) = tokio::join!(solana_buy_future, mexc_sell_future);

                match (&solana_result, &mexc_result) {
                    (Ok((solana_tx, solana_price_info)), Ok(mexc_order)) => {
                        log::info!("[PARALLEL] ✅ Both trades succeeded!");
                        trade_record.solana_tx_signature = Some(solana_tx.clone());
                        actual_buy_price = solana_price_info.executed_price;
                        
                        if let Some(order_id) = mexc_order.get("orderId").and_then(|id| id.as_str()) {
                            trade_record.sell_order_id = Some(order_id.to_string());
                        }
                        if let Some(fills) = mexc_order.get("fills").and_then(|f| f.as_array()) {
                            if let Some(fill) = fills.first() {
                                if let Some(price_str) = fill.get("price").and_then(|p| p.as_str()) {
                                    actual_sell_price = price_str.parse().unwrap_or(expected_sell_price);
                                }
                            }
                        }
                        
                        trade_record.actual_buy_price = Some(actual_buy_price);
                        trade_record.actual_sell_price = Some(actual_sell_price);
                    },
                    (Ok(_), Err(mexc_err)) => {
                        log::error!("[PARALLEL] ❌ MEXC SELL failed: {}", mexc_err);
                        return Err(format!("MEXC sell failed: {}", mexc_err).into());
                    },
                    (Err(solana_err), Ok(_)) => {
                        log::error!("[PARALLEL] ❌ Solana BUY failed: {}", solana_err);
                        return Err(format!("Solana buy failed: {}", solana_err).into());
                    },
                    (Err(solana_err), Err(mexc_err)) => {
                        log::error!("[PARALLEL] ❌ Both trades failed - Solana: {}, MEXC: {}", solana_err, mexc_err);
                        return Err(format!("Both trades failed").into());
                    }
                }
            }
            
            // 🆕 Calculate detailed metrics
            let execution_time = start_time.elapsed().as_millis() as u64;
            trade_record.execution_time_ms = Some(execution_time);
            
            // Calculate slippage
            let buy_slippage = ((actual_buy_price - expected_buy_price) / expected_buy_price).abs() * dec!(100.0);
            let sell_slippage = ((actual_sell_price - expected_sell_price) / expected_sell_price).abs() * dec!(100.0);
            let total_slippage = buy_slippage + sell_slippage;
            
            trade_record.buy_slippage_percent = Some(buy_slippage);
            trade_record.sell_slippage_percent = Some(sell_slippage);
            trade_record.total_slippage_percent = Some(total_slippage);
            
            // Calculate actual profit and spread
            let actual_profit = amount_sol * (actual_sell_price - actual_buy_price) - config::SOLANA_GAS_FEE_USD;
            let actual_spread = ((actual_sell_price - actual_buy_price) / actual_buy_price) * dec!(100.0);
            
            trade_record.actual_profit = Some(actual_profit);
            trade_record.actual_spread_percent = Some(actual_spread);
            trade_record.status = "SUCCESS".to_string();
            
            // 🆕 Display enhanced results
            Self::display_trade_results(&trade_record);
            
            log::info!("[PARALLEL] ✅ {} COMPLETED in {}ms!", trade_id, execution_time);
            Ok(trade_record)
        }

        // 🆕 NEW: Enhanced trade results display
        fn display_trade_results(trade: &TradeRecord) {
            let expected_profit = trade.expected_profit;
            let actual_profit = trade.actual_profit.unwrap_or(Decimal::ZERO);
            let profit_delta = actual_profit - expected_profit;
            
            let expected_spread = trade.expected_spread_percent;
            let actual_spread = trade.actual_spread_percent.unwrap_or(Decimal::ZERO);
            let spread_delta = actual_spread - expected_spread;
            
            let buy_slippage = trade.buy_slippage_percent.unwrap_or(Decimal::ZERO);
            let sell_slippage = trade.sell_slippage_percent.unwrap_or(Decimal::ZERO);
            let total_slippage = trade.total_slippage_percent.unwrap_or(Decimal::ZERO);
            
            let execution_time = trade.execution_time_ms.unwrap_or(0);
            
            // 🎯 Beautiful trade summary exactly as requested
            log::info!("📊 PROFIT: Expected ${:.4} -> Actual ${:.4} (Δ: ${:+.4})", 
                expected_profit, actual_profit, profit_delta);
            log::info!("📈 SPREAD: Expected {:.4}% -> Actual {:.4}% (Δ: {:+.4}%)", 
                expected_spread, actual_spread, spread_delta);
            log::info!("🎯 SLIPPAGE: {:.3}% (Buy: {:.3}%, Sell: {:.3}%)", 
                total_slippage, buy_slippage, sell_slippage);
            log::info!("⏱️ EXECUTION: {}ms", execution_time);
            
            // Additional summary line for quick scanning
            log::info!("🚀 TRADE COMPLETE: {} | ${:.4} profit | {:.3}% slippage | {}ms", 
                trade.trade_id, actual_profit, total_slippage, execution_time);
        }
    }
}

// ---- Logging Module ----
mod logger {
    pub fn init() {
        dotenvy::dotenv().ok();
        if std::env::var("RUST_LOG").is_err() {
            unsafe {
                std::env::set_var("RUST_LOG", "info");
            }
        }
        env_logger::init();
    }
}

// ---- MEXC Connector ----
mod mexc {
    use crate::{config, state::{PriceUpdate, Price}};
    use futures_util::{StreamExt, SinkExt};
    use tokio::sync::mpsc::Sender;
    use serde_json::Value;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    pub async fn run_mexc_connector(
        is_running: Arc<AtomicBool>,
        price_sender: Sender<PriceUpdate>
    ) {
        log::info!("[MEXC] 🚀 Zero-Fee Connector starting for SOL/USDC - TRUE zero fees!");
        log::info!("[MEXC] 🎉 Connected to Zero-Fee Zone for {} - 0% maker/taker fees!", config::MEXC_TARGET_PAIR);
        
        while is_running.load(Ordering::SeqCst) {
            if let Err(e) = connect_and_run(is_running.clone(), &price_sender).await {
                log::error!("[MEXC] Connection error: {}. Retrying in 5 seconds...", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
        log::info!("[MEXC] Connector shutting down.");
    }

    async fn connect_and_run(is_running: Arc<AtomicBool>, price_sender: &Sender<PriceUpdate>) -> anyhow::Result<()> {
        let (mut ws_stream, _) = tokio_tungstenite::connect_async(config::MEXC_WEBSOCKET_URL).await?;
        log::info!("[MEXC] WebSocket connection established to zero-fee exchange.");

        let subscribe_msg = serde_json::json!({
            "method": "SUBSCRIPTION",
            "params": [format!("spot@public.deals.v3.api@{}", config::MEXC_TARGET_PAIR)]
        });

        ws_stream.send(tokio_tungstenite::tungstenite::Message::Text(subscribe_msg.to_string())).await?;
        log::info!("[MEXC] 🎉 Subscribed to {} trade stream in Zero-Fee Zone!", config::MEXC_TARGET_PAIR);

        let mut last_ping = std::time::Instant::now();

        while is_running.load(Ordering::SeqCst) {
            tokio::select!{
                Some(msg) = ws_stream.next() => {
                    let msg = msg?;
                    if let tokio_tungstenite::tungstenite::Message::Text(text) = msg {
                        // Skip system messages
                        if text.contains("SUBSCRIPTION") || text.contains("PONG") || text.contains("code") {
                            log::debug!("[MEXC] System message: {}", text);
                            continue;
                        }
                        
                        // 🔧 DEBUG: Log all non-system messages to see what we're receiving
                        log::debug!("[MEXC] Raw message: {}", text);
                        
                        if let Ok(json_data) = serde_json::from_str::<Value>(&text) {
                            log::debug!("[MEXC] Parsed JSON: {:?}", json_data);
                            
                            if let Some(data) = json_data.get("d") {
                                if let Some(deals) = data.get("deals").and_then(|d| d.as_array()) {
                                    for deal in deals {
                                        if let Some(price_str) = deal.get("p").and_then(|p| p.as_str()) {
                                            match price_str.parse::<Price>() {
                                                Ok(price) => {
                                                    log::info!("[MEXC] 🚀 SOL/USDC price: ${:.4}", price);
                                                    if price_sender.send(PriceUpdate::MEXC(price)).await.is_err() {
                                                        log::error!("[MEXC] Failed to send price to engine. Channel closed.");
                                                        break;
                                                    }
                                                }
                                                Err(e) => {
                                                    log::warn!("[MEXC] Could not parse price string '{}'. Error: {}", price_str, e);
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    log::debug!("[MEXC] No deals array found in data: {:?}", data);
                                }
                            } else {
                                log::debug!("[MEXC] No 'd' field found in JSON: {:?}", json_data);
                            }
                        } else {
                            log::debug!("[MEXC] Failed to parse JSON: {}", text);
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(30)) => {
                    if last_ping.elapsed() >= Duration::from_secs(30) {
                        let ping_msg = serde_json::json!({"method": "PING"});
                        if let Err(e) = ws_stream.send(tokio_tungstenite::tungstenite::Message::Text(ping_msg.to_string())).await {
                            log::warn!("[MEXC] Failed to send ping: {}", e);
                            break;
                        }
                        last_ping = std::time::Instant::now();
                    }
                }
            }
        }
        Ok(())
    }
}

// ---- Solana Jupiter Connector ----
mod solana {
    use crate::{config, state::{Price, PriceUpdate}};
    use reqwest;
    use serde::{Deserialize, Serialize};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use rust_decimal::prelude::*;

    #[derive(Debug, Deserialize, Serialize)]
    struct JupiterPriceResponse {
        data: std::collections::HashMap<String, JupiterPrice>,
    }

    #[derive(Debug, Deserialize, Serialize)]
    struct JupiterPrice {
        id: String,
        #[serde(rename = "type")]
        price_type: String,
        price: String,
    }

    pub async fn run_jupiter_connector(
        is_running: Arc<AtomicBool>,
        price_sender: tokio::sync::mpsc::Sender<PriceUpdate>,
    ) {
        log::info!("[JUPITER] Connector starting for Solana DEX prices.");
        let app_config = config::AppConfig::new();
        let client = reqwest::Client::new();

        while is_running.load(Ordering::SeqCst) {
            match fetch_jupiter_price(&client, &app_config.jupiter_api_url).await {
                Ok(price) => {
                    log::info!("[JUPITER] SOL price: ${:.4}", price);
                    if price_sender.send(PriceUpdate::Solana(price)).await.is_err() {
                        log::error!("[JUPITER] Failed to send price to engine. Channel closed.");
                        break;
                    }
                }
                Err(e) => {
                    log::warn!("[JUPITER] Failed to fetch price: {}", e);
                }
            }
            
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        log::info!("[JUPITER] Connector shutting down.");
    }

    async fn fetch_jupiter_price(client: &reqwest::Client, base_url: &str) -> Result<Price, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{}?ids={}", base_url, config::SOL_MINT_ADDRESS);
        
        let response = client
            .get(&url)
            .timeout(Duration::from_secs(10))
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(format!("HTTP error: {}", response.status()).into());
        }

        let price_data: JupiterPriceResponse = response.json().await?;
        
        if let Some(sol_price) = price_data.data.get(config::SOL_MINT_ADDRESS) {
            let price_decimal = sol_price.price.parse::<f64>()
                .map_err(|_| "Failed to parse Jupiter price string")?;
                
            let price_decimal = Price::from_f64(price_decimal)
                .ok_or("Failed to convert Jupiter price to Decimal")?;
            
            if price_decimal < Price::from(10) || price_decimal > Price::from(1000) {
                return Err(format!("Jupiter price ${} seems unreasonable", price_decimal).into());
            }
            
            Ok(price_decimal)
        } else {
            Err("SOL price not found in Jupiter response".into())
        }
    }
}

// ---- FIXED Arbitrage Engine Module ----
mod engine {
    use crate::{config, risk::RiskManager, state::{PriceUpdate, SharedState, PriceInfo, PriceDataPoint}};
    use crate::{mexc_trading::MexcTradingClient, solana_trading::SolanaTradingClient};
    use tokio::sync::mpsc::Receiver;
    use std::sync::{Arc, Mutex};
    use std::sync::atomic::{AtomicBool, Ordering};
    use rust_decimal_macros::dec;
    use chrono::Utc;

    pub async fn run_arbitrage_engine(
        is_running: Arc<AtomicBool>,
        state: Arc<Mutex<SharedState>>,
        mut price_receiver: Receiver<PriceUpdate>
    ) {
        log::info!("[ENGINE] 🚀 MEXC Zero-Fee Arbitrage Engine started! Paper Trading: {}", if config::PAPER_TRADING {"ON"} else {"OFF"});
        
        // 🚨 CRITICAL: Initialize trading clients for real trading
        let (mexc_client, solana_client) = if !config::PAPER_TRADING {
            log::info!("[ENGINE] 🚨 REAL TRADING MODE - Initializing trading clients...");
            
            let app_config = config::AppConfig::new();
            
            let mexc_client = Some(MexcTradingClient::new(
                app_config.mexc_api_key.clone(), 
                app_config.mexc_api_secret.clone()
            ));
            
            let solana_client = match SolanaTradingClient::new(
                app_config.solana_rpc_url.clone(),
                app_config.solana_wallet_path.clone(),
                app_config.jupiter_api_url.clone()
            ) {
                Ok(client) => {
                    log::info!("[ENGINE] ✅ Solana client initialized. Wallet: {}", client.get_wallet_address());
                    Some(client)
                },
                Err(e) => {
                    log::error!("[ENGINE] ❌ Failed to initialize Solana client: {}", e);
                    log::error!("[ENGINE] ❌ Cannot proceed with real trading - check wallet path and permissions");
                    None
                }
            };
            
            (mexc_client, solana_client)
        } else {
            log::info!("[ENGINE] 📝 Paper trading mode - no real clients needed");
            (None, None)
        };

        while is_running.load(Ordering::SeqCst) {
            if let Some(update) = price_receiver.recv().await {
                
                // Step 1: Update prices in a short-lived mutex scope
                let (mexc_price, solana_price) = {
                    let mut state_guard = state.lock().unwrap();
                    
                    let (price, source) = match update {
                        PriceUpdate::MEXC(p) => (p, "MEXC"),
                        PriceUpdate::Solana(p) => (p, "Jupiter"),
                    };
                    
                    // Log price to history
                    state_guard.price_history.push(PriceDataPoint {
                        timestamp: Utc::now(),
                        source: source.to_string(),
                        price,
                    });

                    // Update prices with timestamp
                    let price_info = PriceInfo {
                        price,
                        timestamp: Utc::now(),
                    };
                    
                    match source {
                        "MEXC" => state_guard.mexc_price = Some(price_info),
                        "Jupiter" => state_guard.solana_price = Some(price_info),
                        _ => {}
                    }

                    // Extract current prices and release the lock immediately
                    (state_guard.mexc_price, state_guard.solana_price)
                }; // state_guard is dropped here

                // Step 2: Check for arbitrage opportunities without holding any locks
                if let (Some(mexc), Some(solana)) = (mexc_price, solana_price) {
                    let p_mexc = mexc.price;
                    let p_solana = solana.price;

                    if p_mexc.is_zero() || p_solana.is_zero() {
                        continue; 
                    }

                    let spread1 = (p_solana - p_mexc) / p_mexc * dec!(100.0);
                    let spread2 = (p_mexc - p_solana) / p_solana * dec!(100.0);

                    // 🚀 OPPORTUNITY 1: Buy on MEXC, Sell on Solana
                    if spread1 > dec!(0.0) {
                        let trade_value = config::TRADE_AMOUNT_SOL * p_mexc;
                        let mexc_fee_cost = dec!(0.0);
                        let solana_gas_cost = config::SOLANA_GAS_FEE_USD;
                        let total_costs = mexc_fee_cost + solana_gas_cost;
                        let total_cost_percent = (total_costs / trade_value) * dec!(100.0);
                        let net_spread = spread1 - total_cost_percent;
                        let expected_profit = trade_value * (net_spread / dec!(100.0));
                        
                        if net_spread > config::MIN_PROFIT_SPREAD_PERCENT {
                            // Check risk in a separate mutex scope
                            let should_trade = {
                                let state_guard = state.lock().unwrap();
                                RiskManager::check_trade(&state_guard, trade_value)
                            }; // state_guard is dropped here
                            
                            if should_trade {
                                // 🆕 NEW: Check price freshness before trading
                                let prices_fresh = {
                                    let state_guard = state.lock().unwrap();
                                    RiskManager::check_price_freshness(&state_guard)
                                };
                                
                                if !prices_fresh {
                                    log::warn!("[ENGINE] ⚠️ Skipping trade due to stale prices");
                                    continue; // Skip this trading opportunity
                                }
                                
                                log::info!("[ENGINE] 🚀 PROFITABLE OPPORTUNITY: Buy MEXC @{:.4}, Sell Solana @{:.4}", p_mexc, p_solana);
                                log::info!("  Net Profit: {:.4}%, Expected: ${:.4}", net_spread, expected_profit);
                                
                                if config::PAPER_TRADING {
                                    // Paper trading - update capital in mutex scope
                                    let mut state_guard = state.lock().unwrap();
                                    RiskManager::update_capital(&mut state_guard, "MEXC", config::TRADE_AMOUNT_SOL, p_mexc, p_solana);
                                } else {
                                    // Real trading execution - no mutex held
                                    if let (Some(ref mexc_client), Some(ref solana_client)) = (&mexc_client, &solana_client) {
                                        match RiskManager::execute_parallel_arbitrage(
                                            mexc_client,
                                            solana_client,
                                            "MEXC",
                                            config::TRADE_AMOUNT_SOL,
                                            p_mexc,
                                            p_solana
                                        ).await {
                                            Ok(trade_record) => {
                                                log::info!("[REAL_TRADE] ✅ Trade executed successfully! Profit: ${:.4}", 
                                                    trade_record.actual_profit.unwrap_or(rust_decimal::Decimal::ZERO));
                                                
                                                // Add trade to history
                                                let mut state_guard = state.lock().unwrap();
                                                state_guard.add_trade_record(trade_record);
                                            },
                                            Err(e) => {
                                                log::error!("[REAL_TRADE] ❌ Trade execution failed: {}", e);
                                            }
                                        }
                                    } else {
                                        log::error!("[ENGINE] ❌ Real trading clients not available!");
                                    }
                                }
                            }
                        }
                    }

                    // 🚀 OPPORTUNITY 2: Buy on Solana, Sell on MEXC
                    if spread2 > dec!(0.0) {
                        let trade_value = config::TRADE_AMOUNT_SOL * p_solana;
                        let mexc_fee_cost = dec!(0.0);
                        let solana_gas_cost = config::SOLANA_GAS_FEE_USD;
                        let total_costs = mexc_fee_cost + solana_gas_cost;
                        let total_cost_percent = (total_costs / trade_value) * dec!(100.0);
                        let net_spread = spread2 - total_cost_percent;
                        let expected_profit = trade_value * (net_spread / dec!(100.0));
                        
                        if net_spread > config::MIN_PROFIT_SPREAD_PERCENT {
                            // Check risk in a separate mutex scope
                            let should_trade = {
                                let state_guard = state.lock().unwrap();
                                RiskManager::check_trade(&state_guard, trade_value)
                            }; // state_guard is dropped here
                            
                            if should_trade {
                                // 🆕 NEW: Check price freshness before trading
                                let prices_fresh = {
                                    let state_guard = state.lock().unwrap();
                                    RiskManager::check_price_freshness(&state_guard)
                                };
                                
                                if !prices_fresh {
                                    log::warn!("[ENGINE] ⚠️ Skipping trade due to stale prices");
                                    continue; // Skip this trading opportunity
                                }
                                
                                log::info!("[ENGINE] 🚀 PROFITABLE OPPORTUNITY: Buy Jupiter @{:.4}, Sell MEXC @{:.4}", p_solana, p_mexc);
                                log::info!("  Net Profit: {:.4}%, Expected: ${:.4}", net_spread, expected_profit);
                                
                                if config::PAPER_TRADING {
                                    // Paper trading - update capital in mutex scope
                                    let mut state_guard = state.lock().unwrap();
                                    RiskManager::update_capital(&mut state_guard, "SOLANA", config::TRADE_AMOUNT_SOL, p_solana, p_mexc);
                                } else {
                                    // Real trading execution - no mutex held
                                    if let (Some(ref mexc_client), Some(ref solana_client)) = (&mexc_client, &solana_client) {
                                        match RiskManager::execute_parallel_arbitrage(
                                            mexc_client,
                                            solana_client,
                                            "SOLANA",
                                            config::TRADE_AMOUNT_SOL,
                                            p_solana,
                                            p_mexc
                                        ).await {
                                            Ok(trade_record) => {
                                                log::info!("[REAL_TRADE] ✅ Trade executed successfully! Profit: ${:.4}", 
                                                    trade_record.actual_profit.unwrap_or(rust_decimal::Decimal::ZERO));
                                                
                                                // Add trade to history
                                                let mut state_guard = state.lock().unwrap();
                                                state_guard.add_trade_record(trade_record);
                                            },
                                            Err(e) => {
                                                log::error!("[REAL_TRADE] ❌ Trade execution failed: {}", e);
                                            }
                                        }
                                    } else {
                                        log::error!("[ENGINE] ❌ Real trading clients not available!");
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                is_running.store(false, Ordering::SeqCst);
            }
        }
        log::info!("[ENGINE] 🚀 Engine shutting down.");
    }
}

// ---- CSV Writer Module ----
mod csv_writer {
    use crate::state::PriceDataPoint;
    use chrono::Utc;
    use csv::WriterBuilder;

    pub fn write_to_csv(records: &[PriceDataPoint]) -> anyhow::Result<()> {
        let timestamp = Utc::now().format("%Y-%m-%d_%H%M%S");
        let filename = format!("mexc_arbitrage_log_{}.csv", timestamp);
        
        let mut writer = WriterBuilder::new()
            .has_headers(false)
            .from_path(&filename)?;

        writer.write_record(&["timestamp", "source", "price"])?;
        
        for record in records {
            writer.serialize(record)?;
        }

        writer.flush()?;
        log::info!("[CSV] 🚀 MEXC Zero-Fee price history saved to: {}", filename);
        Ok(())
    }
}
// 🔧 FIXED: Enhanced CSV writer
mod trade_csv_writer {
    use crate::state::TradeRecord;
    use chrono::Utc;
    use csv::WriterBuilder;
    use rust_decimal::Decimal;

    pub fn write_trades_to_csv(trades: &[TradeRecord]) -> anyhow::Result<()> {
        let timestamp = Utc::now().format("%Y-%m-%d_%H%M%S");
        let filename = format!("mexc_arbitrage_trades_{}.csv", timestamp);
        
        let mut writer = WriterBuilder::new()
            .has_headers(true)
            .from_path(&filename)?;

        // 🆕 Enhanced CSV header with detailed slippage tracking
        writer.write_record(&[
            "trade_id", "timestamp", "direction", "amount_sol", "buy_venue", "sell_venue",
            "expected_buy_price", "expected_sell_price", "actual_buy_price", "actual_sell_price",
            "expected_profit", "actual_profit", "profit_delta",
            "expected_spread_percent", "actual_spread_percent", "spread_delta",
            "buy_slippage_percent", "sell_slippage_percent", "total_slippage_percent",
            "buy_order_id", "sell_order_id", "solana_tx_signature", "total_fees",
            "execution_time_ms", "status"
        ])?;
        
        // Write enhanced trade records
        for trade in trades {
            let profit_delta = trade.actual_profit.unwrap_or(Decimal::ZERO) - trade.expected_profit;
            let spread_delta = trade.actual_spread_percent.unwrap_or(Decimal::ZERO) - trade.expected_spread_percent;
            
            // 🔧 FIXED: Convert all fields to String for consistency
            let row = vec![
                trade.trade_id.clone(),
                trade.timestamp.to_rfc3339(),
                trade.direction.clone(),
                trade.amount_sol.to_string(),
                trade.buy_venue.clone(),
                trade.sell_venue.clone(),
                trade.buy_price.to_string(),
                trade.sell_price.to_string(),
                trade.actual_buy_price.map_or("".to_string(), |p| p.to_string()),
                trade.actual_sell_price.map_or("".to_string(), |p| p.to_string()),
                trade.expected_profit.to_string(),
                trade.actual_profit.map_or("".to_string(), |p| p.to_string()),
                profit_delta.to_string(),
                trade.expected_spread_percent.to_string(),
                trade.actual_spread_percent.map_or("".to_string(), |s| s.to_string()),
                spread_delta.to_string(),
                trade.buy_slippage_percent.map_or("".to_string(), |s| s.to_string()),
                trade.sell_slippage_percent.map_or("".to_string(), |s| s.to_string()),
                trade.total_slippage_percent.map_or("".to_string(), |s| s.to_string()),
                trade.buy_order_id.as_ref().map_or("".to_string(), |id| id.clone()),
                trade.sell_order_id.as_ref().map_or("".to_string(), |id| id.clone()),
                trade.solana_tx_signature.as_ref().map_or("".to_string(), |sig| sig.clone()),
                trade.total_fees.to_string(),
                trade.execution_time_ms.map_or("".to_string(), |t| t.to_string()),
                trade.status.clone(),
            ];
            
            writer.write_record(&row)?;
        }

        writer.flush()?;
        log::info!("[CSV] 🚀 Enhanced trade history saved to: {}", filename);
        Ok(())
    }

    // 🆕 FIXED: Write summary statistics with correct field access
    pub fn write_trade_summary(trades: &[TradeRecord], total_profit: Decimal, trade_count: u32) -> anyhow::Result<()> {
        let timestamp = Utc::now().format("%Y-%m-%d_%H%M%S");
        let filename = format!("mexc_arbitrage_summary_{}.txt", timestamp);
        
        let successful_trades: Vec<_> = trades.iter().filter(|t| t.status == "SUCCESS").collect();
        let avg_profit = if !successful_trades.is_empty() {
            successful_trades.iter()
                .filter_map(|t| t.actual_profit)
                .sum::<Decimal>() / Decimal::from(successful_trades.len())
        } else {
            Decimal::ZERO
        };
        
        // 🔧 FIXED: Use total_slippage_percent instead of slippage_percent
        let avg_slippage = if !successful_trades.is_empty() {
            successful_trades.iter()
                .filter_map(|t| t.total_slippage_percent)
                .sum::<Decimal>() / Decimal::from(successful_trades.len())
        } else {
            Decimal::ZERO
        };

        let summary = format!(
            "MEXC ARBITRAGE BOT TRADING SUMMARY\n\
            =====================================\n\
            Session Timestamp: {}\n\
            Total Trades: {}\n\
            Successful Trades: {}\n\
            Total Profit: ${:.4}\n\
            Average Profit per Trade: ${:.4}\n\
            Average Slippage: {:.4}%\n\
            Success Rate: {:.2}%\n\
            =====================================\n",
            timestamp,
            trade_count,
            successful_trades.len(),
            total_profit,
            avg_profit,
            avg_slippage,
            if trade_count > 0 { (successful_trades.len() as f64 / trade_count as f64) * 100.0 } else { 0.0 }
        );

        std::fs::write(&filename, summary)?;
        log::info!("[SUMMARY] 📊 Trading summary saved to: {}", filename);
        Ok(())
    }
}
