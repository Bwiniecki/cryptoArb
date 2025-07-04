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
use rust_decimal::Decimal;  
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;  // ✅ FIX: Add missing import

// Import modules defined below in this file
use crate::state::SharedState;

// 🚀 UPDATED main() function with automatic balance fetching

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize the logger
    logger::init();

    log::info!("[MAIN] 🚀 MEXC Zero-Fee Implementation - Real Trading Ready!");
    log::info!("[MAIN] 📊 Step 1: Fetching live account balances...");

    // 🆕 NEW: Fetch live balances FIRST before everything else
    let live_balances = match balance_fetcher::BalanceFetcher::fetch_all_balances().await {
        Ok(balances) => {
            log::info!("[MAIN] ✅ Live balances loaded successfully!");
            balances
        },
        Err(e) => {
            log::error!("[MAIN] ❌ Failed to fetch live balances: {}", e);
            log::error!("[MAIN] Cannot proceed without knowing account balances.");
            return Err(anyhow::anyhow!("Failed to fetch live balances: {}", e));  // ✅ FIXED
        }
    };

    // 🆕 NEW: Calculate total capital with current SOL price
    log::info!("[MAIN] 💰 Getting current SOL price for capital calculation...");
    let current_sol_price = match get_initial_sol_price().await {
        Ok(price) => {
            log::info!("[MAIN] 📈 Current SOL price: ${:.4}", price);
            price
        },
        Err(e) => {
            log::warn!("[MAIN] ⚠️ Could not fetch SOL price, using default $150: {}", e);
            dec!(150.0) // Fallback price
        }
    };

    let total_capital = live_balances.total_capital_usd(current_sol_price);
    log::info!("[MAIN] 💎 Total Capital: ${:.4} (MEXC: ${:.4} + {:.6} SOL, Wallet: ${:.4} + {:.6} SOL)", 
        total_capital, 
        live_balances.mexc_usdc, live_balances.mexc_sol,      
        live_balances.wallet_usdc, live_balances.wallet_sol
    );
    // 🆕 NEW: Risk check before starting
    let max_trade_value = total_capital * (config::MAX_CAPITAL_PER_TRADE_PERCENT / dec!(100.0));
    let current_trade_value = config::TRADE_AMOUNT_SOL * current_sol_price;
    
    log::info!("[MAIN] 🛡️  Risk Check: Trade size ${:.2} vs Max allowed ${:.2}", 
        current_trade_value, max_trade_value);
    
    if current_trade_value > max_trade_value {
        log::error!("[MAIN] ❌ RISK VIOLATION: Trade size exceeds maximum allowed!");
        log::error!("[MAIN] Reduce TRADE_AMOUNT_SOL or increase capital.");
        return Err(anyhow::anyhow!("Trade size exceeds risk limits"));
    }

    // The application's run state
    let is_running = Arc::new(AtomicBool::new(true));

    // 🔧 UPDATED: Initialize shared state with LIVE balances
    let shared_state = Arc::new(Mutex::new(SharedState::new_with_live_balances(
        live_balances.mexc_usdc,
        live_balances.mexc_sol,        
        live_balances.wallet_usdc,
        live_balances.wallet_sol,      
        total_capital
    )));

    // Create a channel for price updates
    let (price_sender, price_receiver) = mpsc::channel(128);

    // Clone Arcs for each task
    let engine_state = Arc::clone(&shared_state);
    let final_state_clone = Arc::clone(&shared_state);

    let mexc_running = Arc::clone(&is_running);
    let solana_running = Arc::clone(&is_running);
    let engine_running = Arc::clone(&is_running);
    let failsafe_running = Arc::clone(&is_running);

    log::info!("[MAIN] 🚀 Starting trading threads with live balances...");

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
    log::info!("[MAIN] ⏳ Waiting for initial price feeds...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Task for arbitrage engine
    let engine_handle = tokio::spawn(async move {
        engine::run_arbitrage_engine(engine_running, engine_state, price_receiver).await;
    });

    // ✅ NEW: Task for auto-rebalancing (separate from arbitrage engine)
    let rebalance_state = Arc::clone(&shared_state);
    let rebalance_running = Arc::clone(&is_running);
    let rebalance_handle = tokio::spawn(async move {
        auto_rebalancer::run_rebalance_monitor(rebalance_running, rebalance_state).await;
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

    tokio::select! {
        _ = mexc_handle => log::error!("[MAIN] MEXC connector exited unexpectedly."),
        _ = solana_handle => log::error!("[MAIN] Solana connector exited unexpectedly."),
        _ = engine_handle => log::info!("[MAIN] Arbitrage engine shut down."),
        _ = rebalance_handle => log::info!("[MAIN] Rebalance monitor shut down."), // ✅ ADD THIS LINE
        _ = failsafe_handle => log::info!("[MAIN] Failsafe shutdown triggered."),
    }

    // Ensure all tasks shut down
    is_running.store(false, Ordering::SeqCst);

    // 🆕 NEW: Show final balance comparison
    log::info!("[MAIN] 💰 Fetching final balances for P&L calculation...");
    if let Ok(final_balances) = balance_fetcher::BalanceFetcher::fetch_all_balances().await {
        let final_capital = final_balances.total_capital_usd(current_sol_price);
        let total_pnl = final_capital - total_capital;
        
        log::info!("[MAIN] 📊 FINAL P&L SUMMARY:");
        log::info!("[MAIN] 💰 Starting Capital: ${:.4}", total_capital);
        log::info!("[MAIN] 💰 Final Capital: ${:.4}", final_capital);
        log::info!("[MAIN] 📈 Total P&L: ${:+.4} ({:+.2}%)", 
            total_pnl, (total_pnl / total_capital * dec!(100.0)));
    }
    
    fn log_risk_status(state: &SharedState, sol_price: Decimal) {
        let current_capital = state.get_current_capital(sol_price);
        let session_pnl = state.get_session_pnl(sol_price);
        let session_pnl_percent = state.get_session_pnl_percent(sol_price);
        let max_trade_size = current_capital * (config::MAX_CAPITAL_PER_TRADE_PERCENT / dec!(100.0));
        
        log::info!("[RISK] 📊 Current Status:");
        log::info!("[RISK] 💰 Capital: ${:.4} (Start: ${:.4})", current_capital, state.session_start_capital);
        log::info!("[RISK] 📈 Session P&L: ${:+.4} ({:+.2}%)", session_pnl, session_pnl_percent);
        log::info!("[RISK] 🎯 Max Trade Size: ${:.4} ({}%)", max_trade_size, config::MAX_CAPITAL_PER_TRADE_PERCENT);
        log::info!("[RISK] 🛡️ Drawdown Limit: {:.1}%", config::MAX_DAILY_DRAWDOWN_PERCENT);
    }
    
    // Write price and trade history to CSV (existing code)
    log::info!("[MAIN] 📄 Writing trading history to CSV...");
    let final_state_lock = final_state_clone.lock();
    match final_state_lock {
        Ok(final_state) => {
            // Write price history
            if let Err(e) = csv_writer::write_to_csv(&final_state.price_history) {
                log::error!("[MAIN] Failed to write price CSV: {}", e);
            }
            
            // Write trade history
            if let Err(e) = trade_csv_writer::write_trades_to_csv(&final_state.trade_history) {
                log::error!("[MAIN] Failed to write trades CSV: {}", e);
            } else {
                log::info!("[MAIN] ✅ Successfully wrote trade history to CSV.");
            }
            
            // Write trading summary
            if let Err(e) = trade_csv_writer::write_trade_summary(
                &final_state.trade_history, 
                final_state.total_profit, 
                final_state.trade_count
            ) {
                log::error!("[MAIN] Failed to write summary: {}", e);
            } else {
                log::info!("[MAIN] ✅ Successfully wrote trading summary.");
            }
            
            log::info!("[MAIN] 📊 SESSION STATS: {} trades, ${:.4} total profit", 
                final_state.trade_count, final_state.total_profit);
        },
        Err(poison_error) => {
            log::error!("[MAIN] Mutex was poisoned, cannot write CSV. Error: {}", poison_error);
        }
    }

    log::info!("[MAIN] 🏁 All threads terminated. Program shutdown complete.");
    Ok(())
}

// ---- ENHANCED: Updated get_initial_sol_price with better headers ----
// 🆕 NEW: Helper function to get initial SOL price with Jupiter Pro (ENHANCED)
async fn get_initial_sol_price() -> Result<Decimal> {
    let app_config = config::AppConfig::new();
    
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .default_headers({
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert("X-API-KEY", reqwest::header::HeaderValue::from_str(&app_config.jupiter_api_key).unwrap());
            // 🆕 ADD: Try multiple header formats
            headers.insert("Authorization", reqwest::header::HeaderValue::from_str(&format!("Bearer {}", app_config.jupiter_api_key)).unwrap());
            headers.insert("User-Agent", reqwest::header::HeaderValue::from_str("rust-arbitrage-bot/1.0").unwrap());
            headers
        })
        .build()?;
    
    let url = format!("{}?ids={}", app_config.jupiter_api_url, config::SOL_MINT_ADDRESS);
    
    log::info!("[MAIN] 🚀 Fetching initial SOL price with Pro API key: {}...{}", 
        &app_config.jupiter_api_key[..8], 
        &app_config.jupiter_api_key[app_config.jupiter_api_key.len()-4..]);
    
    let response = client
        .get(&url)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
        
        if status.as_u16() == 429 {
            log::error!("[MAIN] 🚨 Initial price fetch rate limited! Check Pro API key activation.");
            log::error!("[MAIN] 🚨 Response: {}", error_text);
        }
        
        return Err(anyhow::anyhow!("HTTP error ({}): {}", status, error_text));
    }

    let price_data: serde_json::Value = response.json().await?;
    
    if let Some(sol_price) = price_data["data"][config::SOL_MINT_ADDRESS]["price"].as_str() {
        let price_decimal = sol_price.parse::<f64>()
            .map_err(|_| anyhow::anyhow!("Failed to parse Jupiter price string"))?;
            
        let price_decimal = Decimal::from_f64(price_decimal)
            .ok_or_else(|| anyhow::anyhow!("Failed to convert Jupiter price to Decimal"))?;
        
        log::info!("[MAIN] ✅ Initial Pro API price: ${:.4}", price_decimal);
        return Ok(price_decimal);
    }
    
    Err(anyhow::anyhow!("SOL price not found in Jupiter response"))
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
        pub jupiter_api_key: String,  // ✅ NEW: Jupiter Pro API key
        pub solana_rpc_url: String,
        pub solana_wallet_path: String,
    }

    impl AppConfig {
        pub fn new() -> Self {
            Self {
                mexc_api_key: get_env_var("MEXC_API_KEY"),
                mexc_api_secret: get_env_var("MEXC_API_SECRET"),
                jupiter_api_url: get_env_var_optional("JUPITER_API_URL", "https://lite-api.jup.ag/price/v2"),
                jupiter_api_key: get_env_var_optional("JUPITER_API_KEY", "b90e1793-f1e6-441a-94de-8cc0a774d98a"), // ✅ NEW: Your Pro API key
                solana_rpc_url: get_env_var_optional("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com"),
                solana_wallet_path: get_env_var("SOLANA_WALLET_PRIVATE_KEY_PATH"),
            }
        }
    }

    // 🚨 REAL TRADING MODE ENABLED!
    pub const PAPER_TRADING: bool = false;

    // Static config
    pub const MEXC_WEBSOCKET_URL: &str = "wss://wbs.mexc.com/ws";
    pub const MEXC_TARGET_PAIR: &str = "SOLUSDC";
    pub const SOL_MINT_ADDRESS: &str = "So11111111111111111111111111111111111111112";
    pub const USDC_MINT_ADDRESS: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

    // ✅ DEPRECATED: These are now only used for backwards compatibility in paper trading
    // Real trading uses live balances fetched at startup
    pub const MEXC_STARTING_USDC: Decimal = dec!(996.78905698);    // ⚠️ DEPRECATED: Only for paper trading fallback
    pub const WALLET_STARTING_USDC: Decimal = dec!(778.71896);     // ⚠️ DEPRECATED: Only for paper trading fallback

    // ✅ OPTIMIZED: Strategy & Risk - Now uses live capital calculations
    pub const MIN_PROFIT_SPREAD_PERCENT: Decimal = dec!(0.02);     // 0.02% minimum profit
    pub const TRADE_AMOUNT_SOL: Decimal = dec!(1.0);              // 1.0 SOL trades
    pub const MAX_CAPITAL_PER_TRADE_PERCENT: Decimal = dec!(50.0); // 50% max per trade (of LIVE capital)
    pub const MAX_DAILY_DRAWDOWN_PERCENT: Decimal = dec!(20.0);    // 20% max daily loss (from LIVE starting capital)
    
    // Fees and slippage
    pub const SOLANA_GAS_FEE_USD: Decimal = dec!(0.01);
    pub const MAX_EXECUTION_TIME_SECONDS: u64 = 5;
    pub const MAX_PRICE_AGE_SECONDS: u64 = 7;
    pub const SOLANA_MAX_SLIPPAGE_BPS: u16 = 15;
    
    // ✅ NEW: Auto-rebalancing configuration
    pub const AUTO_REBALANCE_ENABLED: bool = true;
    pub const REBALANCE_THRESHOLD_PERCENT: Decimal = dec!(70.0);  // Trigger when 70% or more in one asset
    pub const REBALANCE_TARGET_PERCENT: Decimal = dec!(50.0);     // Target 50/50 split
    pub const REBALANCE_CHECK_INTERVAL_SECONDS: u64 = 15;        // Check every 30 seconds
    pub const REBALANCE_MIN_VALUE_USD: Decimal = dec!(100.0);    // Only rebalance if imbalance > $100  
    pub const REBALANCE_MIN_TRADE_USD: Decimal = dec!(100.0); 
    
    // 🚀 NEW: Jupiter Pro Rate Limiting Configuration
    pub const JUPITER_PRO_RPM_LIMIT: u32 = 600;                    // Total Pro plan limit
    pub const JUPITER_PRICE_UPDATE_RPS: u32 = 2;                   // 3 RPS for price updates
    pub const JUPITER_TRADING_BUDGET_RPS: u32 = 7;                 // 7 RPS for trading calls
    
    // Calculated intervals
    pub const JUPITER_PRICE_INTERVAL_MS: u64 = 1000 / JUPITER_PRICE_UPDATE_RPS as u64; // 333ms
    pub const JUPITER_TRADING_MIN_INTERVAL_MS: u64 = 1000 / JUPITER_TRADING_BUDGET_RPS as u64; // 143ms
    
    // 🔧 EASILY ADJUSTABLE: Change these if you need different balance
    // For more trading headroom: Reduce JUPITER_PRICE_UPDATE_RPS to 2, increase JUPITER_TRADING_BUDGET_RPS to 8
    // For more accurate prices: Increase JUPITER_PRICE_UPDATE_RPS to 4, reduce JUPITER_TRADING_BUDGET_RPS to 6
}


// ---- FINAL FIXED MEXC Trading Execution Module ----
mod mexc_trading {
    use reqwest;
    use serde_json::Value;
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    use rust_decimal::Decimal;
    use std::time::{SystemTime, UNIX_EPOCH};
    use rust_decimal_macros::dec;  // ✅ FIX: Add missing import

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
                            market_price * dec!(1.001) // 0.1% premium for immediate buy
                        } else {
                            market_price * dec!(0.999) // 0.1% discount for immediate sell
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
                current_price * dec!(1.002) // 0.2% premium
            } else {
                // For SELL orders: Accept slightly LESS than market price to ensure execution  
                current_price * dec!(0.998) // 0.2% discount
            };
            
            log::info!("[MEXC] Using execution price: ${:.4} for {} order ({})", 
                execution_price, side, 
                if side == "BUY" { "premium" } else { "discount" });
            
            self.place_order(symbol, side, order_type, quantity, Some(execution_price)).await
        }
    }
}
// 🚀 COMPLETE SOLANA TRADING MODULE - OPTIMIZED FOR HELIUS 50 RPS

mod solana_trading {
    use solana_sdk::{
        signer::{keypair::Keypair, Signer},
        commitment_config::CommitmentConfig,
    };
    use solana_client::rpc_client::RpcClient;
    use std::sync::Arc;
    use base64::Engine;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use rust_decimal::prelude::*;  // ✅ Provides FromPrimitive trait
    use std::time::{Duration, Instant};
    use tokio::time::sleep;
    use crate::config;  // ✅ Import config module

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
        last_rpc_call: Arc<std::sync::Mutex<Instant>>,
        min_rpc_interval: Duration,
    }

    impl SolanaTradingClient {
        pub fn new(rpc_url: String, wallet_path: String, jupiter_api_key: String) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
            // 🚀 OPTIMIZED: Use confirmed commitment for better rate limits
            let client = Arc::new(RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed()));
            
            let wallet_data = std::fs::read_to_string(wallet_path)?;
            let wallet_bytes: Vec<u8> = serde_json::from_str(&wallet_data)?;
            let wallet = Arc::new(Keypair::from_bytes(&wallet_bytes)?);

            // 🚀 OPTIMIZED: HTTP client with connection pooling and retries
            let http_client = reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .pool_idle_timeout(Duration::from_secs(30))
                .pool_max_idle_per_host(50)
                .tcp_keepalive(Duration::from_secs(30))
                .default_headers({
                    let mut headers = reqwest::header::HeaderMap::new();
                    
                    // 🔧 TRY MULTIPLE HEADER FORMATS - Jupiter might expect different ones
                    headers.insert("X-API-KEY", reqwest::header::HeaderValue::from_str(&jupiter_api_key).unwrap());
                    headers.insert("Authorization", reqwest::header::HeaderValue::from_str(&format!("Bearer {}", jupiter_api_key)).unwrap());
                    headers.insert("x-api-key", reqwest::header::HeaderValue::from_str(&jupiter_api_key).unwrap()); // lowercase
                    headers.insert("api-key", reqwest::header::HeaderValue::from_str(&jupiter_api_key).unwrap()); // alternative
                    headers.insert("User-Agent", reqwest::header::HeaderValue::from_str("rust-arbitrage-bot/1.0").unwrap());
                    
                    log::info!("[SOLANA] 🚀 HTTP client initialized with Pro API headers");
                    log::info!("[SOLANA] 🔑 Using API key: {}...{}", &jupiter_api_key[..8], &jupiter_api_key[jupiter_api_key.len()-4..]);
                    
                    headers
                })
                .build()?;

            Ok(Self {
                client,
                wallet,
                http_client,
                last_rpc_call: Arc::new(std::sync::Mutex::new(Instant::now() - Duration::from_secs(1))),
                min_rpc_interval: Duration::from_millis(10), // ✅ UPGRADED: 10ms for 600 RPM (6x faster!)
            })
        }

        pub fn get_wallet_address(&self) -> String {
            self.wallet.pubkey().to_string()
        }

        // ✅ UPGRADED: Rate limit optimized for Jupiter Pro 600 RPM
        async fn rate_limit_check(&self) -> Duration {
            let last_call = self.last_rpc_call.lock().unwrap();
            let elapsed = last_call.elapsed();
            
            if elapsed < self.min_rpc_interval {
                let wait_time = self.min_rpc_interval.saturating_sub(elapsed);
                log::debug!("[SOLANA] 🚀 Rate limiting: waiting {}ms (Jupiter Pro 600 RPM optimized)", wait_time.as_millis());
                wait_time
            } else {
                Duration::from_millis(0) // No wait needed
            }
        }

        // ✅ HELPER: Update rate limit timestamp
        fn update_rate_limit_timestamp(&self) {
            let mut last_call = self.last_rpc_call.lock().unwrap();
            *last_call = Instant::now();
        }

        // 🔧 OPTIMIZED: Rate-limited RPC calls
        async fn get_latest_blockhash_with_rate_limit(&self) -> Result<solana_sdk::hash::Hash, Box<dyn std::error::Error + Send + Sync>> {
            // Check rate limit
            let wait_time = self.rate_limit_check().await;
            if wait_time > Duration::from_millis(0) {
                sleep(wait_time).await;
            }

            // Update timestamp
            self.update_rate_limit_timestamp();

            // Execute with retry logic
            let mut retries = 3;
            loop {
                match self.client.get_latest_blockhash() {
                    Ok(result) => return Ok(result),
                    Err(e) => {
                        if retries > 0 && e.to_string().contains("429") {
                            retries -= 1;
                            log::warn!("[SOLANA] 🔄 Blockhash rate limited, retrying in 0.5s... ({} retries left)", retries);
                            sleep(Duration::from_millis(500)).await;  // ✅ FASTER: 0.5s retries
                            continue;
                        }
                        return Err(format!("Get blockhash failed: {}", e).into());
                    }
                }
            }
        }

        async fn send_transaction_with_rate_limit(&self, transaction: &solana_sdk::transaction::VersionedTransaction) -> Result<solana_sdk::signature::Signature, Box<dyn std::error::Error + Send + Sync>> {
            // Check rate limit
            let wait_time = self.rate_limit_check().await;
            if wait_time > Duration::from_millis(0) {
                sleep(wait_time).await;
            }

            // Update timestamp
            self.update_rate_limit_timestamp();

            // Execute with retry logic
            let mut retries = 3;
            loop {
                match self.client.send_transaction(transaction) {
                    Ok(result) => return Ok(result),
                    Err(e) => {
                        if retries > 0 && e.to_string().contains("429") {
                            retries -= 1;
                            log::warn!("[SOLANA] 🔄 Send transaction rate limited, retrying in 0.5s... ({} retries left)", retries);
                            sleep(Duration::from_millis(500)).await;
                            continue;
                        }
                        return Err(format!("Send transaction failed: {}", e).into());
                    }
                }
            }
        }

        async fn send_legacy_transaction_with_rate_limit(&self, transaction: &solana_sdk::transaction::Transaction) -> Result<solana_sdk::signature::Signature, Box<dyn std::error::Error + Send + Sync>> {
            // Check rate limit
            let wait_time = self.rate_limit_check().await;
            if wait_time > Duration::from_millis(0) {
                sleep(wait_time).await;
            }

            // Update timestamp
            self.update_rate_limit_timestamp();

            // Execute with retry logic
            let mut retries = 3;
            loop {
                match self.client.send_transaction(transaction) {
                    Ok(result) => return Ok(result),
                    Err(e) => {
                        if retries > 0 && e.to_string().contains("429") {
                            retries -= 1;
                            log::warn!("[SOLANA] 🔄 Send legacy transaction rate limited, retrying in 0.5s... ({} retries left)", retries);
                            sleep(Duration::from_millis(500)).await;
                            continue;
                        }
                        return Err(format!("Send legacy transaction failed: {}", e).into());
                    }
                }
            }
        }

        // ✅ PUBLIC: Get SOL balance (used by balance fetcher)
        pub async fn get_sol_balance(
            &self,
            pubkey: &solana_sdk::pubkey::Pubkey
        ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
            
            // Check rate limit
            let wait_time = self.rate_limit_check().await;
            if wait_time > Duration::from_millis(0) {
                sleep(wait_time).await;
            }

            // Update timestamp
            self.update_rate_limit_timestamp();

            // Execute with retry logic
            let mut retries = 3;
            loop {
                match self.client.get_balance(pubkey) {
                    Ok(balance) => return Ok(balance),
                    Err(e) => {
                        if retries > 0 && e.to_string().contains("429") {
                            retries -= 1;
                            log::warn!("[SOLANA] 🔄 Balance fetch rate limited, retrying in 0.5s... ({} left)", retries);
                            sleep(Duration::from_millis(500)).await;
                            continue;
                        }
                        return Err(format!("Failed to get SOL balance: {}", e).into());
                    }
                }
            }
        }

        // ✅ PUBLIC: Get USDC balance (SINGLE IMPLEMENTATION)
        pub async fn get_usdc_balance(
            &self,
            wallet_address: &str
        ) -> Result<Decimal, Box<dyn std::error::Error + Send + Sync>> {
            
            let request_body = serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getTokenAccountsByOwner",
                "params": [
                    wallet_address,
                    {
                        "mint": config::USDC_MINT_ADDRESS
                    },
                    {
                        "encoding": "jsonParsed"
                    }
                ]
            });

            // Check rate limit
            let wait_time = self.rate_limit_check().await;
            if wait_time > Duration::from_millis(0) {
                sleep(wait_time).await;
            }

            // Update timestamp
            self.update_rate_limit_timestamp();

            // Use Helius RPC for better performance
            let app_config = config::AppConfig::new();
            let response = self.http_client
                .post(&app_config.solana_rpc_url)  // ✅ Use your Helius RPC
                .json(&request_body)
                .send()
                .await?;

            if !response.status().is_success() {
                return Err("Failed to fetch USDC balance".into());
            }

            let data: serde_json::Value = response.json().await?;
            
            if let Some(accounts) = data["result"]["value"].as_array() {
                if let Some(account) = accounts.first() {
                    if let Some(balance_value) = account["account"]["data"]["parsed"]["info"]["tokenAmount"]["uiAmount"].as_f64() {
                        return Ok(Decimal::from_f64(balance_value).unwrap_or(Decimal::ZERO));  // ✅ FIXED: FromPrimitive trait
                    }
                }
            }
            
            // If no USDC token account exists, balance is 0
            Ok(Decimal::ZERO)
        }

        pub async fn execute_swap(
            &self,
            input_mint: &str,
            output_mint: &str,
            amount: u64,
            slippage_bps: u16,
        ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
            
            log::info!("[SOLANA] 🚀 Starting Jupiter swap: {} {} to {}", amount, 
                if input_mint.contains("So1111") { "lamports SOL" } else { "lamports USDC" },
                if output_mint.contains("So1111") { "SOL" } else { "USDC" });
            
            // 🔧 STEP 1: Get quote with retry logic
            let quote_data = self.get_jupiter_quote(input_mint, output_mint, amount, slippage_bps).await?;
            
            // 🔧 STEP 2: Get swap transaction with retry logic  
            let transaction_data = self.get_jupiter_swap_transaction(&quote_data).await?;
            
            // 🔧 STEP 3: Process and send transaction with rate limiting
            self.process_and_send_transaction(&transaction_data).await
        }

        async fn get_jupiter_quote(
            &self,
            input_mint: &str,
            output_mint: &str,
            amount: u64,
            slippage_bps: u16,
        ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
            
            let quote_url = format!(
                "https://quote-api.jup.ag/v6/quote?inputMint={}&outputMint={}&amount={}&slippageBps={}&onlyDirectRoutes=true&maxAccounts=20",
                input_mint, output_mint, amount, slippage_bps
            );

            log::debug!("[SOLANA] 🚀 Getting Jupiter quote with Pro API (600 RPM)");

            // ✅ UPGRADED: Retry logic optimized for Pro plan
            let mut retries = 5; // More retries since we have higher limits
            loop {
                let response = tokio::time::timeout(
                    Duration::from_secs(8),
                    self.http_client.get(&quote_url).send() // ✅ Pro API key automatically included in headers
                ).await;

                match response {
                    Ok(Ok(resp)) if resp.status().is_success() => {
                        let quote_data: serde_json::Value = resp.json().await
                            .map_err(|e| format!("Failed to parse quote response: {}", e))?;
                        log::debug!("[SOLANA] ✅ Jupiter Pro quote received");
                        return Ok(quote_data);
                    },
                    Ok(Ok(resp)) => {
                        let status = resp.status();
                        let error_text = resp.text().await.unwrap_or_else(|_| "Unknown error".to_string());
                        
                        if status.as_u16() == 429 && retries > 0 {
                            retries -= 1;
                            log::warn!("[SOLANA] 🔄 Jupiter Pro rate limited (unusual!), retrying in 0.5s... ({} retries left)", retries);
                            sleep(Duration::from_millis(500)).await; // ✅ FASTER: 0.5s retries for Pro
                            continue;
                        }
                        
                        return Err(format!("Jupiter Pro quote failed ({}): {}", status, error_text).into());
                    },
                    Ok(Err(e)) => {
                        if retries > 0 {
                            retries -= 1;
                            log::warn!("[SOLANA] 🔄 Jupiter Pro quote request failed, retrying... Error: {}", e);
                            sleep(Duration::from_millis(200)).await; // ✅ FASTER: 0.2s retries
                            continue;
                        }
                        return Err(format!("Jupiter Pro quote request failed: {}", e).into());
                    },
                    Err(_) => {
                        if retries > 0 {
                            retries -= 1;
                            log::warn!("[SOLANA] 🔄 Jupiter Pro quote timeout, retrying...");
                            sleep(Duration::from_millis(200)).await; // ✅ FASTER: 0.2s retries
                            continue;
                        }
                        return Err("Jupiter Pro quote timeout".into());
                    }
                }
            }
        }

        async fn get_jupiter_swap_transaction(
            &self,
            quote_data: &serde_json::Value,
        ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
            
            let swap_request = serde_json::json!({
                "userPublicKey": self.wallet.pubkey().to_string(),
                "quoteResponse": quote_data,
                "wrapAndUnwrapSol": true,
                "dynamicComputeUnitLimit": true,
                "prioritizationFeeLamports": 20000,  // 🚀 INCREASED: Even higher priority to reduce failures
                "asLegacyTransaction": false,
                "skipUserAccountsRpcCalls": true,
            });

            log::debug!("[SOLANA] 🚀 Getting swap transaction with Pro API (high priority)");

            // ✅ UPGRADED: Retry logic optimized for Pro plan
            let mut retries = 5; // More retries since we have higher limits
            loop {
                let response = tokio::time::timeout(
                    Duration::from_secs(10),
                    self.http_client
                        .post("https://quote-api.jup.ag/v6/swap")
                        .json(&swap_request)
                        .send() // ✅ Pro API key automatically included in headers
                ).await;

                match response {
                    Ok(Ok(resp)) if resp.status().is_success() => {
                        let swap_data: serde_json::Value = resp.json().await
                            .map_err(|e| format!("Failed to parse swap response: {}", e))?;
                        
                        let transaction_data = swap_data["swapTransaction"].as_str()
                            .ok_or("No swap transaction in response")?;
                        
                        log::debug!("[SOLANA] ✅ Jupiter Pro swap transaction received, size: {} chars", transaction_data.len());
                        return Ok(transaction_data.to_string());
                    },
                    Ok(Ok(resp)) => {
                        let status = resp.status();
                        let error_text = resp.text().await.unwrap_or_else(|_| "Unknown error".to_string());
                        
                        if status.as_u16() == 429 && retries > 0 {
                            retries -= 1;
                            log::warn!("[SOLANA] 🔄 Jupiter Pro swap rate limited (unusual!), retrying in 0.5s... ({} retries left)", retries);
                            sleep(Duration::from_millis(500)).await; // ✅ FASTER: 0.5s retries for Pro
                            continue;
                        }
                        
                        return Err(format!("Jupiter Pro swap failed ({}): {}", status, error_text).into());
                    },
                    Ok(Err(e)) => {
                        if retries > 0 {
                            retries -= 1;
                            log::warn!("[SOLANA] 🔄 Jupiter Pro swap request failed, retrying... Error: {}", e);
                            sleep(Duration::from_millis(200)).await; // ✅ FASTER: 0.2s retries
                            continue;
                        }
                        return Err(format!("Jupiter Pro swap request failed: {}", e).into());
                    },
                    Err(_) => {
                        if retries > 0 {
                            retries -= 1;
                            log::warn!("[SOLANA] 🔄 Jupiter Pro swap timeout, retrying...");
                            sleep(Duration::from_millis(200)).await; // ✅ FASTER: 0.2s retries
                            continue;
                        }
                        return Err("Jupiter Pro swap timeout".into());
                    }
                }
            }
        }

        async fn process_and_send_transaction(
            &self,
            transaction_data: &str,
        ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
            
            let transaction_bytes = base64::prelude::BASE64_STANDARD.decode(transaction_data)
                .map_err(|e| format!("Failed to decode transaction: {}", e))?;

            log::debug!("[SOLANA] 🔧 Transaction bytes decoded, size: {} bytes", transaction_bytes.len());

            if transaction_bytes.len() > 1232 {
                return Err("Transaction too large for Solana network".into());
            }

            // Try versioned transaction first
            match bincode::deserialize::<solana_sdk::transaction::VersionedTransaction>(&transaction_bytes) {
                Ok(mut versioned_tx) => {
                    log::debug!("[SOLANA] ✅ Using versioned transaction");
                    
                    // 🔧 FIXED: Use specific rate-limited function
                    let recent_blockhash = self.get_latest_blockhash_with_rate_limit().await?;
                    
                    // Set blockhash
                    match &mut versioned_tx.message {
                        solana_sdk::message::VersionedMessage::Legacy(ref mut legacy_msg) => {
                            legacy_msg.recent_blockhash = recent_blockhash;
                        },
                        solana_sdk::message::VersionedMessage::V0(ref mut v0_msg) => {
                            v0_msg.recent_blockhash = recent_blockhash;
                        }
                    }
                    
                    // Sign transaction
                    let signed_tx = solana_sdk::transaction::VersionedTransaction::try_new(
                        versioned_tx.message.clone(),
                        &[self.wallet.as_ref()]
                    ).map_err(|e| format!("Failed to sign versioned transaction: {}", e))?;
                    
                    // 🔧 FIXED: Use specific rate-limited function
                    let signature = self.send_transaction_with_rate_limit(&signed_tx).await?;
                    
                    log::info!("[SOLANA] ✅ Versioned transaction sent! Signature: {}", signature);
                    Ok(signature.to_string())
                },
                Err(_) => {
                    // Fallback to legacy transaction
                    log::debug!("[SOLANA] 🔄 Falling back to legacy transaction");
                    let mut transaction: solana_sdk::transaction::Transaction = bincode::deserialize(&transaction_bytes)
                        .map_err(|e| format!("Failed to deserialize legacy transaction: {}", e))?;
                    
                    // 🔧 FIXED: Use specific rate-limited function
                    let recent_blockhash = self.get_latest_blockhash_with_rate_limit().await?;
                    
                    transaction.message.recent_blockhash = recent_blockhash;
                    transaction.try_sign(&[self.wallet.as_ref()], recent_blockhash)
                        .map_err(|e| format!("Failed to sign transaction: {}", e))?;

                    // 🔧 FIXED: Use specific rate-limited function
                    let signature = self.send_legacy_transaction_with_rate_limit(&transaction).await?;
                    
                    log::info!("[SOLANA] ✅ Legacy transaction sent! Signature: {}", signature);
                    Ok(signature.to_string())
                }
            }
        }

        pub async fn execute_swap_with_price_tracking(
            &self,
            input_mint: &str,
            output_mint: &str,
            amount: u64,
            slippage_bps: u16,
        ) -> Result<(String, SwapPriceInfo), Box<dyn std::error::Error + Send + Sync>> {
            
            log::info!("[SOLANA] 🚀 Starting Jupiter swap with price tracking: {} {} to {}", amount, 
                if input_mint.contains("So1111") { "lamports SOL" } else { "lamports USDC" },
                if output_mint.contains("So1111") { "SOL" } else { "USDC" });
            
            // Get quote for price calculation
            let quote_data = self.get_jupiter_quote(input_mint, output_mint, amount, slippage_bps).await?;
            
            // Extract expected amounts for price calculation
            let in_amount = quote_data["inAmount"].as_str()
                .ok_or("Missing inAmount")?.parse::<u64>()?;
            let out_amount = quote_data["outAmount"].as_str()
                .ok_or("Missing outAmount")?.parse::<u64>()?;

            log::debug!("[SOLANA] Quote: {} -> {} (expected)", in_amount, out_amount);

            // Execute the swap
            let signature = self.execute_swap(input_mint, output_mint, amount, slippage_bps).await?;
            
            // Calculate executed price
            let executed_price = if input_mint.contains("So1111") {
                // SOL -> USDC: price = USDC_out / SOL_in
                Decimal::from(out_amount) / Decimal::from(1_000_000) / (Decimal::from(in_amount) / Decimal::from(1_000_000_000))
            } else {
                // USDC -> SOL: price = USDC_in / SOL_out  
                Decimal::from(in_amount) / Decimal::from(1_000_000) / (Decimal::from(out_amount) / Decimal::from(1_000_000_000))
            };
            
            let price_info = SwapPriceInfo {
                executed_price,
                slippage_percent: Decimal::from(slippage_bps) / dec!(100.0),
            };
            
            log::info!("[SOLANA] ✅ Swap with tracking executed at ${:.4} per SOL", executed_price);
            Ok((signature, price_info))
        }
    }
}

// ---- FIXED Shared State Module ----
mod state {
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;  // ✅ FIX: Add missing import
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
        pub mexc_sol: Decimal,     
        pub wallet_usdc: Decimal,
        pub wallet_sol: Decimal,   
    }

    #[derive(Debug, Clone)]
    pub struct SharedState {
        pub mexc_price: Option<PriceInfo>,
        pub solana_price: Option<PriceInfo>,
        pub starting_capital: Decimal,        // ✅ NOW: Live fetched starting capital
        pub session_start_capital: Decimal,   // ✅ NEW: Capital at session start (for P&L)
        pub balances: AccountBalance,
        pub price_history: Vec<PriceDataPoint>,
        pub trade_history: Vec<TradeRecord>,
        pub total_profit: Decimal,
        pub trade_count: u32,
        pub last_rebalance_check: chrono::DateTime<chrono::Utc>, // ✅ NEW: Track last rebalance check
    }

    // 🔧 UPDATED SharedState implementation to work with live balances

    impl SharedState {
        // ✅ ENHANCED: Constructor with complete live balances
        pub fn new_with_live_balances(
            mexc_usdc: Decimal, 
            mexc_sol: Decimal,        // ✅ ADD: MEXC SOL
            wallet_usdc: Decimal,
            wallet_sol: Decimal,      // ✅ ADD: Wallet SOL
            total_starting_capital: Decimal
        ) -> Self {
            let balances = AccountBalance {
                mexc_usdc,
                mexc_sol,     // ✅ ADD: Track MEXC SOL
                wallet_usdc,
                wallet_sol,   // ✅ ADD: Track Wallet SOL
            };
            
            log::info!("[STATE] 🆕 Initialized with complete live balances:");
            log::info!("[STATE] 💰 MEXC - USDC: ${:.4}, SOL: {:.6}", mexc_usdc, mexc_sol);
            log::info!("[STATE] 🏦 Wallet - USDC: ${:.4}, SOL: {:.6}", wallet_usdc, wallet_sol);
            log::info!("[STATE] 💎 Total Starting Capital: ${:.4}", total_starting_capital);
            
            Self {
                mexc_price: None,
                solana_price: None,
                starting_capital: total_starting_capital,       // ✅ Live starting capital
                session_start_capital: total_starting_capital,  // ✅ Session start for P&L
                balances,
                price_history: Vec::new(),
                trade_history: Vec::new(),
                total_profit: Decimal::ZERO,
                trade_count: 0,
                last_rebalance_check: chrono::Utc::now(), // ✅ NEW: Initialize rebalance timer
            }
        }

        // ✅ BACKWARDS COMPATIBILITY: Original constructor (now uses live balance fetching)
        pub fn new() -> Self {
            log::warn!("[STATE] ⚠️ Using legacy constructor - should use new_with_live_balances()");
            
            // ✅ FALLBACK: Use hardcoded values only as last resort
            let balances = AccountBalance {
                mexc_usdc: config::MEXC_STARTING_USDC,
                mexc_sol: Decimal::ZERO,    // ✅ Default to zero
                wallet_usdc: config::WALLET_STARTING_USDC,
                wallet_sol: Decimal::ZERO,  // ✅ Default to zero
            };
            let starting_capital = balances.mexc_usdc + balances.wallet_usdc;
            
            Self {
                mexc_price: None,
                solana_price: None,
                starting_capital,
                session_start_capital: starting_capital,
                balances,
                price_history: Vec::new(),
                trade_history: Vec::new(),
                total_profit: Decimal::ZERO,
                trade_count: 0,
                last_rebalance_check: chrono::Utc::now(), // ✅ NEW: Initialize rebalance timer
            }
        }
        
        // ✅ ENHANCED: Update live balances during trading
        pub fn update_live_balances(
            &mut self, 
            mexc_usdc: Decimal, 
            mexc_sol: Decimal,
            wallet_usdc: Decimal,
            wallet_sol: Decimal,
            sol_price: Decimal
        ) {
            let old_capital = self.get_current_capital(sol_price);
            
            self.balances.mexc_usdc = mexc_usdc;
            self.balances.mexc_sol = mexc_sol;
            self.balances.wallet_usdc = wallet_usdc;
            self.balances.wallet_sol = wallet_sol;
            
            let new_capital = self.get_current_capital(sol_price);
            let capital_change = new_capital - old_capital;
            
            log::info!("[STATE] 💰 Balance update: Capital changed by ${:+.4}", capital_change);
            log::info!("[STATE] 📊 New total capital: ${:.4}", new_capital);
        }

        // ✅ ENHANCED: Get current capital including all assets
        pub fn get_current_capital(&self, sol_price: Decimal) -> Decimal {
            self.balances.mexc_usdc + 
            (self.balances.mexc_sol * sol_price) +
            self.balances.wallet_usdc + 
            (self.balances.wallet_sol * sol_price)
        }

        // ✅ LEGACY: Get current capital (USDC only) for backwards compatibility
        pub fn get_current_capital_legacy(&self) -> Decimal {
            self.balances.mexc_usdc + self.balances.wallet_usdc
        }

        pub fn add_trade_record(&mut self, trade: TradeRecord) {
            if let Some(actual_profit) = trade.actual_profit {
                self.total_profit += actual_profit;
            }
            self.trade_count += 1;
            self.trade_history.push(trade);
        }

        // ✅ NEW: Calculate session P&L
        pub fn get_session_pnl(&self, sol_price: Decimal) -> Decimal {
            self.get_current_capital(sol_price) - self.session_start_capital
        }

        // ✅ NEW: Calculate session P&L percentage
        pub fn get_session_pnl_percent(&self, sol_price: Decimal) -> Decimal {
            let pnl = self.get_session_pnl(sol_price);
            if self.session_start_capital.is_zero() {
                Decimal::ZERO
            } else {
                (pnl / self.session_start_capital) * dec!(100.0)  // ✅ FIX: Now imports dec! properly
            }
        }

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

        // ✅ NEW: Auto-rebalancing methods
        pub fn needs_rebalancing(&self, sol_price: Decimal) -> Option<RebalanceAction> {
            if !config::AUTO_REBALANCE_ENABLED {
                return None;
            }

            // Check both MEXC and wallet separately
            let mexc_action = self.check_mexc_rebalance_need(sol_price);
            let wallet_action = self.check_wallet_rebalance_need(sol_price);

            // Return the first rebalance action needed (prioritize MEXC for speed)
            mexc_action.or(wallet_action)
        }

        fn check_mexc_rebalance_need(&self, sol_price: Decimal) -> Option<RebalanceAction> {
            let mexc_usdc_value = self.balances.mexc_usdc;
            let mexc_sol_value = self.balances.mexc_sol * sol_price;
            let mexc_total = mexc_usdc_value + mexc_sol_value;

            if mexc_total < config::REBALANCE_MIN_VALUE_USD {
                return None; // Not enough value to matter
            }

            let usdc_percentage = (mexc_usdc_value / mexc_total) * dec!(100.0);
            let sol_percentage = (mexc_sol_value / mexc_total) * dec!(100.0);

            if usdc_percentage >= config::REBALANCE_THRESHOLD_PERCENT {
                // Too much USDC, need to buy SOL
                let target_usdc_value = mexc_total * (config::REBALANCE_TARGET_PERCENT / dec!(100.0));
                let usdc_to_convert = mexc_usdc_value - target_usdc_value;
                
                Some(RebalanceAction {
                    venue: RebalanceVenue::MEXC,
                    action_type: RebalanceType::UsdcToSol,
                    amount: usdc_to_convert,
                    reason: format!("MEXC USDC: {:.1}% (target: 50%)", usdc_percentage),
                })
            } else if sol_percentage >= config::REBALANCE_THRESHOLD_PERCENT {
                // Too much SOL, need to sell SOL
                let target_sol_value = mexc_total * (config::REBALANCE_TARGET_PERCENT / dec!(100.0));
                let sol_to_convert = (mexc_sol_value - target_sol_value) / sol_price;
                
                Some(RebalanceAction {
                    venue: RebalanceVenue::MEXC,
                    action_type: RebalanceType::SolToUsdc,
                    amount: sol_to_convert,
                    reason: format!("MEXC SOL: {:.1}% (target: 50%)", sol_percentage),
                })
            } else {
                None
            }
        }

        fn check_wallet_rebalance_need(&self, sol_price: Decimal) -> Option<RebalanceAction> {
            let wallet_usdc_value = self.balances.wallet_usdc;
            let wallet_sol_value = self.balances.wallet_sol * sol_price;
            let wallet_total = wallet_usdc_value + wallet_sol_value;

            if wallet_total < config::REBALANCE_MIN_VALUE_USD {
                return None; // Not enough value to matter
            }

            let usdc_percentage = (wallet_usdc_value / wallet_total) * dec!(100.0);
            let sol_percentage = (wallet_sol_value / wallet_total) * dec!(100.0);

            if usdc_percentage >= config::REBALANCE_THRESHOLD_PERCENT {
                // Too much USDC, need to buy SOL
                let target_usdc_value = wallet_total * (config::REBALANCE_TARGET_PERCENT / dec!(100.0));
                let usdc_to_convert = wallet_usdc_value - target_usdc_value;
                
                Some(RebalanceAction {
                    venue: RebalanceVenue::Wallet,
                    action_type: RebalanceType::UsdcToSol,
                    amount: usdc_to_convert,
                    reason: format!("Wallet USDC: {:.1}% (target: 50%)", usdc_percentage),
                })
            } else if sol_percentage >= config::REBALANCE_THRESHOLD_PERCENT {
                // Too much SOL, need to sell SOL
                let target_sol_value = wallet_total * (config::REBALANCE_TARGET_PERCENT / dec!(100.0));
                let sol_to_convert = (wallet_sol_value - target_sol_value) / sol_price;
                
                Some(RebalanceAction {
                    venue: RebalanceVenue::Wallet,
                    action_type: RebalanceType::SolToUsdc,
                    amount: sol_to_convert,
                    reason: format!("Wallet SOL: {:.1}% (target: 50%)", sol_percentage),
                })
            } else {
                None
            }
        }

        pub fn update_rebalance_check_time(&mut self) {
            self.last_rebalance_check = chrono::Utc::now();
        }

        pub fn should_check_rebalance(&self) -> bool {
            let now = chrono::Utc::now();
            let elapsed = now.signed_duration_since(self.last_rebalance_check);
            elapsed.num_seconds() >= config::REBALANCE_CHECK_INTERVAL_SECONDS as i64
        }
    }

    #[derive(Debug)]
    pub enum PriceUpdate {
        MEXC(Price),
        Solana(Price),
    }

    // ✅ NEW: Auto-rebalancing structures
    #[derive(Debug, Clone)]
    pub struct RebalanceAction {
        pub venue: RebalanceVenue,
        pub action_type: RebalanceType,
        pub amount: Decimal,
        pub reason: String,
    }

    #[derive(Debug, Clone)]
    pub enum RebalanceVenue {
        MEXC,
        Wallet,
    }

    #[derive(Debug, Clone)]
    pub enum RebalanceType {
        UsdcToSol,
        SolToUsdc,
    }
}
// 🔧 COMPLETE RISK MODULE with all imports and beautiful trade output
mod risk {
    use crate::{config, state::{SharedState, TradeRecord}, mexc_trading::MexcTradingClient, solana_trading::{SolanaTradingClient, SwapPriceInfo}};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use rust_decimal::prelude::*; // This provides ToPrimitive trait
    use chrono::Utc;
    use std::time::Instant;

    pub struct RiskManager;

    impl RiskManager {
        // ✅ ENHANCED: Check trade with dynamic capital calculation
        pub fn check_trade(state: &SharedState, trade_value_usd: Decimal, current_sol_price: Decimal) -> bool {
            let current_capital = state.get_current_capital(current_sol_price);

            let max_trade_value = current_capital * (config::MAX_CAPITAL_PER_TRADE_PERCENT / dec!(100.0));
            if trade_value_usd > max_trade_value {
                log::warn!("[RISK] VETO: Trade value ${:.4} exceeds max size ${:.4} ({}% of ${:.4})", 
                    trade_value_usd, max_trade_value, config::MAX_CAPITAL_PER_TRADE_PERCENT, current_capital);
                return false;
            }

            // ✅ FIXED: Use session start capital for drawdown calculation (not hardcoded values)
            let session_pnl_percent = state.get_session_pnl_percent(current_sol_price);
            
            // If we have a negative P&L (loss), check if it exceeds our drawdown limit
            if session_pnl_percent < Decimal::ZERO {
                let drawdown_percent = session_pnl_percent.abs(); // Convert to positive for comparison
                
                if drawdown_percent > config::MAX_DAILY_DRAWDOWN_PERCENT {
                    log::error!("[RISK] 🚨 CRITICAL: Session drawdown of {:.2}% exceeds limit of {:.2}%!", 
                        drawdown_percent, config::MAX_DAILY_DRAWDOWN_PERCENT);
                    log::error!("[RISK] 📊 Session P&L: ${:+.4} ({:+.2}%)", 
                        state.get_session_pnl(current_sol_price), session_pnl_percent);
                    return false;
                }
            }

            // ✅ NEW: Log current risk status
            log::debug!("[RISK] ✅ Trade approved - Size: ${:.4}/{:.4}, Session P&L: {:+.2}%/{:.1}%", 
                trade_value_usd, max_trade_value, session_pnl_percent, config::MAX_DAILY_DRAWDOWN_PERCENT);
            
            true
        }

        // ✅ ENHANCED: Check price freshness (unchanged)
        pub fn check_price_freshness(state: &SharedState) -> bool {
            if !state.are_prices_fresh() {
                log::warn!("[RISK] VETO: Prices are stale - aborting trade");
                return false;
            }
            true
        }

        // ✅ ENHANCED: Update capital for paper trading with SOL price awareness
        pub fn update_capital(
            state: &mut SharedState, 
            venue_bought: &str, 
            amount_sol: Decimal, 
            price_bought: Decimal, 
            price_sold: Decimal,
            current_sol_price: Decimal
        ) {
            // 🚨 CRITICAL: This should only run in paper trading mode
            if !config::PAPER_TRADING {
                log::error!("[REAL_TRADE] ❌ update_capital() called but real trades should use execute_parallel_arbitrage()!");
                return;
            }

            // Paper trading logic (legacy USDC-only tracking)
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

            log::info!("[PAPER_TRADE] 🚀 ZERO-FEE SOL/USDC Profit: ${:.4}. New Capital: ${:.4}", 
                profit, state.get_current_capital(current_sol_price));
        }

        // 🚀 COMPLETE Enhanced execution function with beautiful trade output
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
                trade_record.buy_slippage_percent = Some(dec!(0.01));
                trade_record.sell_slippage_percent = Some(dec!(0.015));
                trade_record.total_slippage_percent = Some(dec!(0.025));
                trade_record.execution_time_ms = Some(1200);
                trade_record.status = "SUCCESS".to_string();
                trade_record.actual_buy_price = Some(expected_buy_price * dec!(1.0001));
                trade_record.actual_sell_price = Some(expected_sell_price * dec!(0.9999));
                
                // 🎯 BEAUTIFUL TRADE OUTPUT - Paper Trading
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
                
                // 🔧 CRITICAL FIX: Correct lamports conversion
                let sol_lamports = (amount_sol * dec!(1_000_000_000)).to_u64().unwrap_or(0);
                log::info!("[PARALLEL] 🔧 FIXED: Converting {} SOL to {} lamports", amount_sol, sol_lamports);
                log::info!("[PARALLEL] MEXC BUY {} SOL + Solana SELL {} lamports", amount_sol, sol_lamports);
                
                let solana_sell_future = solana_client.execute_swap_with_price_tracking(
                    config::SOL_MINT_ADDRESS,
                    config::USDC_MINT_ADDRESS,
                    sol_lamports,
                    50, // 🚀 SPEED: 0.5% slippage for reliability
                );

                let (mexc_result, solana_result): (
                    Result<serde_json::Value, _>, 
                    Result<(String, SwapPriceInfo), _>
                ) = tokio::join!(mexc_buy_future, solana_sell_future);

                match (&mexc_result, &solana_result) {
                    (Ok(mexc_order), Ok((solana_tx, solana_price_info))) => {
                        log::info!("[PARALLEL] ✅ Both trades succeeded!");
                        
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
                let usdc_lamports = (amount_sol * expected_buy_price * dec!(1_000_000)).to_u64().unwrap_or(0);
                log::info!("[PARALLEL] 🔧 FIXED: Converting ${} to {} USDC lamports", amount_sol * expected_buy_price, usdc_lamports);
                log::info!("[PARALLEL] Solana BUY {} USDC lamports + MEXC SELL {} SOL", usdc_lamports, amount_sol);
                
                let solana_buy_future = solana_client.execute_swap_with_price_tracking(
                    config::USDC_MINT_ADDRESS,
                    config::SOL_MINT_ADDRESS,
                    usdc_lamports,
                    50, // 🚀 SPEED: 0.5% slippage for reliability
                );
                
                let mexc_sell_future = mexc_client.place_order_with_current_price(
                    "SOLUSDC", "SELL", "MARKET", amount_sol, Some(expected_sell_price)
                );

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
            
            // 🎯 BEAUTIFUL TRADE OUTPUT - Real Trading
            Self::display_trade_results(&trade_record);
            
            log::info!("[PARALLEL] ✅ {} COMPLETED in {}ms!", trade_id, execution_time);
            Ok(trade_record)
        }

        // 🎯 BEAUTIFUL TRADE RESULTS DISPLAY - Your Requested Output
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
// ---- FIXED Solana Jupiter Connector with Pro API Headers ----
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
        log::info!("[JUPITER] 🚀 Pro Connector starting for Solana DEX prices (600 RPM).");
        
        // 🔧 CRITICAL FIX: Create client with Pro API headers
        let app_config = config::AppConfig::new();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .default_headers({
                let mut headers = reqwest::header::HeaderMap::new();
                headers.insert("X-API-KEY", reqwest::header::HeaderValue::from_str(&app_config.jupiter_api_key).unwrap());
                // 🆕 ADD: Alternative header format in case Jupiter expects this
                headers.insert("Authorization", reqwest::header::HeaderValue::from_str(&format!("Bearer {}", app_config.jupiter_api_key)).unwrap());
                headers.insert("User-Agent", reqwest::header::HeaderValue::from_str("rust-arbitrage-bot/1.0").unwrap());
                headers
            })
            .build()
            .expect("Failed to build Jupiter Pro client");

        log::info!("[JUPITER] ✅ Pro client initialized with API key: {}...{}", 
            &app_config.jupiter_api_key[..8], 
            &app_config.jupiter_api_key[app_config.jupiter_api_key.len()-4..]);

        while is_running.load(Ordering::SeqCst) {
            match fetch_jupiter_price(&client, &app_config.jupiter_api_url).await {
                Ok(price) => {
                    log::info!("[JUPITER] 🚀 SOL price: ${:.4} (Pro API)", price);
                    if price_sender.send(PriceUpdate::Solana(price)).await.is_err() {
                        log::error!("[JUPITER] Failed to send price to engine. Channel closed.");
                        break;
                    }
                }
                Err(e) => {
                    log::warn!("[JUPITER] Pro API failed to fetch price: {}", e);
                    // 🆕 ADD: Check if it's a rate limit error specifically
                    if e.to_string().contains("429") || e.to_string().contains("rate limit") {
                        log::error!("[JUPITER] 🚨 RATE LIMITED on Pro plan! Check API key activation.");
                        log::error!("[JUPITER] 🚨 API Key being used: {}...{}", 
                            &app_config.jupiter_api_key[..8], 
                            &app_config.jupiter_api_key[app_config.jupiter_api_key.len()-4..]);
                    }
                }
            }
            
            // 🔧 ADJUSTED: Faster polling for Pro plan (600 RPM = 10 RPS = 0.1s intervals)
            tokio::time::sleep(Duration::from_millis(config::JUPITER_PRICE_INTERVAL_MS)).await;
        }
        log::info!("[JUPITER] Pro connector shutting down.");
    }

    async fn fetch_jupiter_price(client: &reqwest::Client, base_url: &str) -> Result<Price, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{}?ids={}", base_url, config::SOL_MINT_ADDRESS);
        
        log::debug!("[JUPITER] 🚀 Fetching price with Pro API headers");
        
        // 🔧 CRITICAL FIX: Client already has Pro headers built in
        let response = client
            .get(&url)
            .timeout(Duration::from_secs(10))
            .send() // ✅ Now includes Pro API headers!
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
            
            // 🆕 ENHANCED: Better error reporting for rate limits
            if status.as_u16() == 429 {
                log::error!("[JUPITER] 🚨 Rate limited on Pro plan! Status: {}, Response: {}", status, error_text);
                return Err(format!("Pro plan rate limited ({}): {}", status, error_text).into());
            }
            
            return Err(format!("HTTP error ({}): {}", status, error_text).into());
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
            
            log::debug!("[JUPITER] ✅ Pro API price parsed: ${:.4}", price_decimal);
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
                app_config.jupiter_api_key.clone()
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
                                RiskManager::check_trade(&state_guard, trade_value, p_mexc)
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
                                    RiskManager::update_capital(&mut state_guard, "MEXC", config::TRADE_AMOUNT_SOL, p_mexc, p_solana, p_mexc);
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
                                RiskManager::check_trade(&state_guard, trade_value, p_mexc) // ✅ Pass current SOL price
                            };
                            
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
                                    RiskManager::update_capital(&mut state_guard, "SOLANA", config::TRADE_AMOUNT_SOL, p_mexc, p_solana, p_mexc);
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
// 🚀 NEW: Automatic Balance Fetcher Module

mod balance_fetcher {
    use crate::{config, mexc_trading::MexcTradingClient, solana_trading::SolanaTradingClient};
    use rust_decimal::Decimal; 
    use rust_decimal_macros::dec;
    use rust_decimal::prelude::*;

    #[derive(Debug, Clone)]
    pub struct LiveBalances {
        pub mexc_usdc: Decimal,
        pub mexc_sol: Decimal,
        pub wallet_usdc: Decimal,
        pub wallet_sol: Decimal,
    }

    impl LiveBalances {
        pub fn total_capital_usd(&self, sol_price: Decimal) -> Decimal {
            self.mexc_usdc + 
            (self.mexc_sol * sol_price) +
            self.wallet_usdc + 
            (self.wallet_sol * sol_price)
        }
    }

    pub struct BalanceFetcher;

    impl BalanceFetcher {
        /// 🚀 Main function: Fetch all account balances before starting trading
        pub async fn fetch_all_balances() -> Result<LiveBalances, Box<dyn std::error::Error + Send + Sync>> {
            log::info!("[BALANCE] 🔍 Fetching live account balances...");
            
            let app_config = config::AppConfig::new();
            
            // Initialize clients
            let mexc_client = MexcTradingClient::new(
                app_config.mexc_api_key.clone(),
                app_config.mexc_api_secret.clone()
            );
            
            // 🔧 CRITICAL FIX: Pass jupiter_api_key instead of jupiter_api_url
            let solana_client = SolanaTradingClient::new(
                app_config.solana_rpc_url.clone(),
                app_config.solana_wallet_path.clone(),
                app_config.jupiter_api_key.clone()  // ✅ FIXED: Was jupiter_api_url, now jupiter_api_key
            )?;

            // Log which API key we're using for verification
            log::info!("[BALANCE] 🔑 Using Jupiter Pro API key: {}...{}", 
                &app_config.jupiter_api_key[..8], 
                &app_config.jupiter_api_key[app_config.jupiter_api_key.len()-4..]);

            // Fetch balances in parallel for speed
            log::info!("[BALANCE] 📊 Fetching MEXC and Solana balances in parallel...");
            
            let mexc_future = Self::fetch_mexc_balances(&mexc_client);
            let solana_future = Self::fetch_solana_balances(&solana_client);
            
            let (mexc_result, solana_result) = tokio::join!(mexc_future, solana_future);
            
            // Process results
            let (mexc_usdc, mexc_sol) = mexc_result?;
            let (wallet_usdc, wallet_sol) = solana_result?;
            
            let balances = LiveBalances {
                mexc_usdc,
                mexc_sol,
                wallet_usdc,
                wallet_sol,
            };
            
            // Enhanced balance summary
            log::info!("[BALANCE] ✅ Live balances fetched successfully!");
            log::info!("[BALANCE] 💰 MEXC USDC: ${:.4}", balances.mexc_usdc);
            log::info!("[BALANCE] 🪙 MEXC SOL: {:.6} SOL", balances.mexc_sol);
            log::info!("[BALANCE] 🏦 Wallet USDC: ${:.4}", balances.wallet_usdc);
            log::info!("[BALANCE] 🪙 Wallet SOL: {:.6} SOL", balances.wallet_sol);
            
            Ok(balances)
        }

        /// Fetch both MEXC USDC and SOL balances
        async fn fetch_mexc_balances(
            mexc_client: &MexcTradingClient
        ) -> Result<(Decimal, Decimal), Box<dyn std::error::Error + Send + Sync>> {
            
            log::debug!("[BALANCE] 🔍 Fetching MEXC account balances (USDC + SOL)...");
            
            let account_data = mexc_client.get_account_balance().await?;
            
            let mut mexc_usdc = Decimal::ZERO;
            let mut mexc_sol = Decimal::ZERO;
            
            // Parse MEXC response to find both USDC and SOL balances
            if let Some(balances) = account_data.get("balances").and_then(|b| b.as_array()) {
                for balance in balances {
                    if let Some(asset) = balance.get("asset").and_then(|a| a.as_str()) {
                        if let Some(free_str) = balance.get("free").and_then(|f| f.as_str()) {
                            let free_balance = free_str.parse::<Decimal>()
                                .map_err(|_| format!("Failed to parse MEXC {} balance", asset))?;
                            
                            match asset {
                                "USDC" => {
                                    mexc_usdc = free_balance;
                                    log::debug!("[BALANCE] ✅ MEXC USDC balance: ${:.4}", free_balance);
                                },
                                "SOL" => {
                                    mexc_sol = free_balance;
                                    log::debug!("[BALANCE] ✅ MEXC SOL balance: {:.6} SOL", free_balance);
                                },
                                _ => {} // Skip other assets
                            }
                        }
                    }
                }
            }
            
            log::debug!("[BALANCE] 📊 MEXC Totals - USDC: ${:.4}, SOL: {:.6}", mexc_usdc, mexc_sol);
            Ok((mexc_usdc, mexc_sol))
        }

        /// Fetch Solana wallet balances (USDC + SOL)
        async fn fetch_solana_balances(
            solana_client: &SolanaTradingClient
        ) -> Result<(Decimal, Decimal), Box<dyn std::error::Error + Send + Sync>> {
            
            log::debug!("[BALANCE] 🔍 Fetching Solana wallet balances...");
            
            let wallet_address = solana_client.get_wallet_address();
            
            // Fetch both SOL and USDC balances in parallel
            let sol_future = Self::fetch_sol_balance(solana_client, &wallet_address);
            let usdc_future = Self::fetch_usdc_balance(solana_client, &wallet_address);
            
            let (sol_result, usdc_result) = tokio::join!(sol_future, usdc_future);
            
            let sol_balance = sol_result?;
            let usdc_balance = usdc_result?;
            
            log::debug!("[BALANCE] ✅ Solana SOL: {:.6}, USDC: ${:.4}", sol_balance, usdc_balance);
            Ok((usdc_balance, sol_balance))
        }

        /// Fetch SOL balance using RPC
        async fn fetch_sol_balance(
            solana_client: &SolanaTradingClient,
            wallet_address: &str
        ) -> Result<Decimal, Box<dyn std::error::Error + Send + Sync>> {
            
            // Use the client's internal RPC to get SOL balance
            let pubkey = wallet_address.parse::<solana_sdk::pubkey::Pubkey>()
                .map_err(|_| "Invalid wallet address")?;
            
            let balance_lamports = solana_client.get_sol_balance(&pubkey).await?;
            
            // Convert lamports to SOL (1 SOL = 1,000,000,000 lamports)
            let sol_balance = Decimal::from(balance_lamports) / dec!(1_000_000_000);
            
            Ok(sol_balance)
        }

        /// Fetch USDC balance using the client's method
        async fn fetch_usdc_balance(
            solana_client: &SolanaTradingClient,
            wallet_address: &str
        ) -> Result<Decimal, Box<dyn std::error::Error + Send + Sync>> {
            
            // Use the client's get_usdc_balance method (now with correct API key)
            let usdc_balance = solana_client.get_usdc_balance(wallet_address).await?;
            
            Ok(usdc_balance)
        }
    }
}
// ✅ NEW: Dedicated Auto-Rebalancing Module  
// ✅ FIXED Auto-Rebalancing Module  
// ✅ FIXED: Auto-Rebalancing Module with Better Logic
mod auto_rebalancer {
    use crate::{config, state::SharedState, mexc_trading::MexcTradingClient, solana_trading::SolanaTradingClient, balance_fetcher};
    use std::sync::{Arc, Mutex};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use rust_decimal::prelude::*;

    pub async fn run_rebalance_monitor(
        is_running: Arc<AtomicBool>,
        state: Arc<Mutex<SharedState>>,
    ) {
        log::info!("[REBALANCE] 🔄 Auto-rebalancing monitor started (checking every {}s)", config::REBALANCE_CHECK_INTERVAL_SECONDS);
        log::info!("[REBALANCE] ⚙️ Config: {}% trigger, {}% target, ${:.0} min value", 
            config::REBALANCE_THRESHOLD_PERCENT, config::REBALANCE_TARGET_PERCENT, config::REBALANCE_MIN_VALUE_USD);
        
        // Initialize trading clients for rebalancing
        let app_config = config::AppConfig::new();
        
        let mexc_client = MexcTradingClient::new(
            app_config.mexc_api_key.clone(),
            app_config.mexc_api_secret.clone()
        );
        
        // 🔧 FIXED: Pass jupiter_api_key instead of jupiter_api_url
        let solana_client = match SolanaTradingClient::new(
            app_config.solana_rpc_url.clone(),
            app_config.solana_wallet_path.clone(),
            app_config.jupiter_api_key.clone()  // ✅ FIXED: Correct parameter
        ) {
            Ok(client) => {
                log::info!("[REBALANCE] ✅ Solana client initialized for rebalancing");
                Some(client)
            },
            Err(e) => {
                log::error!("[REBALANCE] ❌ Failed to initialize Solana client: {}", e);
                None
            }
        };
        
        while is_running.load(Ordering::SeqCst) {
            // ✅ FIXED: Use correct interval from config
            tokio::time::sleep(Duration::from_secs(config::REBALANCE_CHECK_INTERVAL_SECONDS)).await;
            
            if !is_running.load(Ordering::SeqCst) {
                break;
            }
            
            log::info!("[REBALANCE] 🔍 Starting rebalance check cycle...");
            
            // Get current SOL price
            let current_sol_price = match get_current_sol_price().await {
                Ok(price) => {
                    log::debug!("[REBALANCE] 📈 Current SOL price: ${:.4}", price);
                    price
                },
                Err(e) => {
                    log::warn!("[REBALANCE] ⚠️ Could not get SOL price for rebalancing: {}", e);
                    continue;
                }
            };
            
            // Fetch fresh balances for rebalancing check
            match balance_fetcher::BalanceFetcher::fetch_all_balances().await {
                Ok(live_balances) => {
                    log::debug!("[REBALANCE] ✅ Fresh balances fetched successfully");
                    
                    // Update state with fresh balances
                    {
                        let mut state_guard = match state.lock() {
                            Ok(guard) => guard,
                            Err(_) => {
                                log::debug!("[REBALANCE] ⏭️ Skipping - state locked");
                                continue;
                            }
                        };
                        state_guard.update_live_balances(
                            live_balances.mexc_usdc,
                            live_balances.mexc_sol,
                            live_balances.wallet_usdc,
                            live_balances.wallet_sol,
                            current_sol_price
                        );
                    }
                    
                    // Check for rebalancing needs
                    execute_rebalancing_check(&live_balances, &mexc_client, &solana_client, current_sol_price).await;
                },
                Err(e) => {
                    log::warn!("[REBALANCE] ⚠️ Could not fetch balances for rebalancing: {}", e);
                }
            }
        }
        
        log::info!("[REBALANCE] 🔄 Rebalancing monitor shutting down");
    }
    
    async fn execute_rebalancing_check(
        balances: &balance_fetcher::LiveBalances,
        mexc_client: &MexcTradingClient,
        solana_client: &Option<SolanaTradingClient>,
        sol_price: Decimal,
    ) {
        if !config::AUTO_REBALANCE_ENABLED {
            log::debug!("[REBALANCE] ⏭️ Auto-rebalancing disabled in config");
            return;
        }
        
        log::info!("[REBALANCE] 🔍 Checking balance ratios...");
        
        // 🔧 FIXED: Check MEXC rebalancing with better logic
        check_mexc_rebalancing(balances, mexc_client, sol_price).await;
        
        // 🔧 FIXED: Check Wallet rebalancing with better logic  
        check_wallet_rebalancing(balances, solana_client, sol_price).await;
    }
    
    async fn check_mexc_rebalancing(
        balances: &balance_fetcher::LiveBalances,
        mexc_client: &MexcTradingClient,
        sol_price: Decimal,
    ) {
        let mexc_usdc_value = balances.mexc_usdc;
        let mexc_sol_value = balances.mexc_sol * sol_price;
        let mexc_total = mexc_usdc_value + mexc_sol_value;
        
        if mexc_total < config::REBALANCE_MIN_VALUE_USD {
            log::debug!("[REBALANCE] ⏭️ MEXC total value ${:.2} below minimum ${:.2}", mexc_total, config::REBALANCE_MIN_VALUE_USD);
            return;
        }
        
        let mexc_usdc_percent = (mexc_usdc_value / mexc_total) * dec!(100.0);
        let mexc_sol_percent = (mexc_sol_value / mexc_total) * dec!(100.0);
        
        log::info!("[REBALANCE] 📊 MEXC: {:.1}% USDC (${:.2}) / {:.1}% SOL (${:.2})", 
            mexc_usdc_percent, mexc_usdc_value, mexc_sol_percent, mexc_sol_value);
        
        // ✅ FIXED: Better rebalancing logic with deadband
        if mexc_usdc_percent >= config::REBALANCE_THRESHOLD_PERCENT {
            // Too much USDC, buy SOL
            let target_usdc_value = mexc_total * (config::REBALANCE_TARGET_PERCENT / dec!(100.0));
            let usdc_to_convert = mexc_usdc_value - target_usdc_value;
            
            if usdc_to_convert >= config::REBALANCE_MIN_TRADE_USD {
                let sol_to_buy = usdc_to_convert / sol_price;
                
                log::info!("[REBALANCE] 🔄 MEXC: Too much USDC ({:.1}%), buying {:.4} SOL with ${:.2}", 
                    mexc_usdc_percent, sol_to_buy, usdc_to_convert);
                
                if let Err(e) = mexc_client.place_order_with_current_price(
                    "SOLUSDC", "BUY", "MARKET", sol_to_buy, None
                ).await {
                    log::error!("[REBALANCE] ❌ MEXC buy failed: {}", e);
                } else {
                    log::info!("[REBALANCE] ✅ MEXC rebalance completed: Bought {:.4} SOL", sol_to_buy);
                }
            } else {
                log::debug!("[REBALANCE] ⏭️ MEXC trade too small: ${:.2} < ${:.2}", usdc_to_convert, config::REBALANCE_MIN_TRADE_USD);
            }
        } else if mexc_sol_percent >= config::REBALANCE_THRESHOLD_PERCENT {
            // Too much SOL, sell SOL
            let target_sol_value = mexc_total * (config::REBALANCE_TARGET_PERCENT / dec!(100.0));
            let sol_value_to_convert = mexc_sol_value - target_sol_value;
            
            if sol_value_to_convert >= config::REBALANCE_MIN_TRADE_USD {
                let sol_to_sell = sol_value_to_convert / sol_price;
                
                log::info!("[REBALANCE] 🔄 MEXC: Too much SOL ({:.1}%), selling {:.4} SOL for ${:.2}", 
                    mexc_sol_percent, sol_to_sell, sol_value_to_convert);
                
                if let Err(e) = mexc_client.place_order_with_current_price(
                    "SOLUSDC", "SELL", "MARKET", sol_to_sell, None
                ).await {
                    log::error!("[REBALANCE] ❌ MEXC sell failed: {}", e);
                } else {
                    log::info!("[REBALANCE] ✅ MEXC rebalance completed: Sold {:.4} SOL", sol_to_sell);
                }
            } else {
                log::debug!("[REBALANCE] ⏭️ MEXC trade too small: ${:.2} < ${:.2}", sol_value_to_convert, config::REBALANCE_MIN_TRADE_USD);
            }
        } else {
            log::info!("[REBALANCE] ✅ MEXC is balanced: {:.1}% USDC / {:.1}% SOL", mexc_usdc_percent, mexc_sol_percent);
        }
    }
    
    async fn check_wallet_rebalancing(
        balances: &balance_fetcher::LiveBalances,
        solana_client: &Option<SolanaTradingClient>,
        sol_price: Decimal,
    ) {
        let wallet_usdc_value = balances.wallet_usdc;
        let wallet_sol_value = balances.wallet_sol * sol_price;
        let wallet_total = wallet_usdc_value + wallet_sol_value;
        
        if wallet_total < config::REBALANCE_MIN_VALUE_USD {
            log::debug!("[REBALANCE] ⏭️ Wallet total value ${:.2} below minimum ${:.2}", wallet_total, config::REBALANCE_MIN_VALUE_USD);
            return;
        }
        
        let wallet_usdc_percent = (wallet_usdc_value / wallet_total) * dec!(100.0);
        let wallet_sol_percent = (wallet_sol_value / wallet_total) * dec!(100.0);
        
        log::info!("[REBALANCE] 📊 Wallet: {:.1}% USDC (${:.2}) / {:.1}% SOL (${:.2})", 
            wallet_usdc_percent, wallet_usdc_value, wallet_sol_percent, wallet_sol_value);
        
        match solana_client {
            Some(solana) => {
                // ✅ FIXED: Better rebalancing logic with deadband
                if wallet_usdc_percent >= config::REBALANCE_THRESHOLD_PERCENT {
                    // Too much USDC, buy SOL
                    let target_usdc_value = wallet_total * (config::REBALANCE_TARGET_PERCENT / dec!(100.0));
                    let usdc_to_convert = wallet_usdc_value - target_usdc_value;
                    
                    if usdc_to_convert >= config::REBALANCE_MIN_TRADE_USD {
                        let usdc_lamports = (usdc_to_convert * dec!(1_000_000)).to_u64().unwrap_or(0);
                        
                        log::info!("[REBALANCE] 🔄 Wallet: Too much USDC ({:.1}%), swapping ${:.2} to SOL", 
                            wallet_usdc_percent, usdc_to_convert);
                        
                        if let Err(e) = solana.execute_swap(
                            config::USDC_MINT_ADDRESS,
                            config::SOL_MINT_ADDRESS,
                            usdc_lamports,
                            100, // 1% slippage for rebalancing
                        ).await {
                            log::error!("[REBALANCE] ❌ Wallet USDC->SOL swap failed: {}", e);
                        } else {
                            log::info!("[REBALANCE] ✅ Wallet rebalance completed: Swapped ${:.2} USDC to SOL", usdc_to_convert);
                        }
                    } else {
                        log::debug!("[REBALANCE] ⏭️ Wallet trade too small: ${:.2} < ${:.2}", usdc_to_convert, config::REBALANCE_MIN_TRADE_USD);
                    }
                } else if wallet_sol_percent >= config::REBALANCE_THRESHOLD_PERCENT {
                    // Too much SOL, sell SOL
                    let target_sol_value = wallet_total * (config::REBALANCE_TARGET_PERCENT / dec!(100.0));
                    let sol_value_to_convert = wallet_sol_value - target_sol_value;
                    
                    if sol_value_to_convert >= config::REBALANCE_MIN_TRADE_USD {
                        let sol_to_convert = sol_value_to_convert / sol_price;
                        let sol_lamports = (sol_to_convert * dec!(1_000_000_000)).to_u64().unwrap_or(0);
                        
                        log::info!("[REBALANCE] 🔄 Wallet: Too much SOL ({:.1}%), swapping {:.4} SOL to USDC", 
                            wallet_sol_percent, sol_to_convert);
                        
                        if let Err(e) = solana.execute_swap(
                            config::SOL_MINT_ADDRESS,
                            config::USDC_MINT_ADDRESS,
                            sol_lamports,
                            100, // 1% slippage for rebalancing
                        ).await {
                            log::error!("[REBALANCE] ❌ Wallet SOL->USDC swap failed: {}", e);
                        } else {
                            log::info!("[REBALANCE] ✅ Wallet rebalance completed: Swapped {:.4} SOL to USDC", sol_to_convert);
                        }
                    } else {
                        log::debug!("[REBALANCE] ⏭️ Wallet trade too small: ${:.2} < ${:.2}", sol_value_to_convert, config::REBALANCE_MIN_TRADE_USD);
                    }
                } else {
                    log::info!("[REBALANCE] ✅ Wallet is balanced: {:.1}% USDC / {:.1}% SOL", wallet_usdc_percent, wallet_sol_percent);
                }
            },
            None => {
                log::error!("[REBALANCE] ❌ Solana client not available for wallet rebalancing!");
            }
        }
    }
    
    async fn get_current_sol_price() -> Result<Decimal, Box<dyn std::error::Error + Send + Sync>> {
        let app_config = config::AppConfig::new();
        
        let client = reqwest::Client::builder()
            .default_headers({
                let mut headers = reqwest::header::HeaderMap::new();
                headers.insert("X-API-KEY", reqwest::header::HeaderValue::from_str(&app_config.jupiter_api_key).unwrap());
                headers
            })
            .build()?;
        
        let url = format!("{}?ids={}", app_config.jupiter_api_url, config::SOL_MINT_ADDRESS);
        
        let response = client
            .get(&url)
            .timeout(Duration::from_secs(10))
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(format!("HTTP error: {}", response.status()).into());
        }

        let price_data: serde_json::Value = response.json().await?;
        
        if let Some(sol_price) = price_data["data"][config::SOL_MINT_ADDRESS]["price"].as_str() {
            let price_decimal = sol_price.parse::<f64>()
                .map_err(|_| "Failed to parse Jupiter price string")?;
                
            let price_decimal = Decimal::from_f64(price_decimal)
                .ok_or("Failed to convert Jupiter price to Decimal")?;
            
            return Ok(price_decimal);
        }
        
        Err("SOL price not found in Jupiter response".into())
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
