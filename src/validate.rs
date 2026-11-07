//! Path parameter checks, run before anything is forwarded. They check the raw
//! (percent-encoded) segment, and none of the allowed characters is `/`, `.`
//! or `%`, so a parameter can't redirect a request to another upstream path.

use uuid::Uuid;

use crate::error::GatewayError;

/// Checks one `{name}` path parameter. Unknown names are rejected.
pub fn path_param(name: &'static str, raw: &str) -> Result<(), GatewayError> {
    let (valid, expected) = match name {
        "id" => (is_uuid(raw), "a UUID"),
        "funding_id" => (is_uuid(raw), "a UUID"),
        "address" => (is_base58_address(raw), "a base58 Solana address"),
        "number" => (is_season_number(raw), "a season number"),
        "token" | "wallet" => (is_evm_address(raw), "a 0x-prefixed 20-byte EVM address"),
        _ => (false, "a known parameter"),
    };
    if valid {
        Ok(())
    } else {
        Err(GatewayError::InvalidPathParam { name, expected })
    }
}

/// Whether `name` has a check in [`path_param`].
pub fn is_known(name: &str) -> bool {
    matches!(
        name,
        "id" | "funding_id" | "number" | "address" | "token" | "wallet"
    )
}

/// Bot, entrant, order and bot wallet funding ids, in any form the `uuid`
/// crate parses.
pub fn is_uuid(raw: &str) -> bool {
    Uuid::try_parse(raw).is_ok()
}

/// A Solana address: 32 to 44 base58 characters that decode to a 32-byte key.
pub fn is_base58_address(raw: &str) -> bool {
    (32..=44).contains(&raw.len())
        && matches!(bs58::decode(raw).into_vec(), Ok(bytes) if bytes.len() == 32)
}

/// A Robinhood Chain (EVM) token or wallet address: `0x` and 40 hex digits,
/// in any case. The hood API lowercases it.
pub fn is_evm_address(raw: &str) -> bool {
    raw.strip_prefix("0x")
        .is_some_and(|hex| hex.len() == 40 && hex.bytes().all(|b| b.is_ascii_hexdigit()))
}

/// Arena season numbers (32-bit integers).
pub fn is_season_number(raw: &str) -> bool {
    raw.parse::<i32>().is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_accept_the_standard_uuid_forms() {
        for raw in [
            "1b4b1a48-0e19-4a5c-9f5e-7c7a0f2b3d10",
            "1B4B1A48-0E19-4A5C-9F5E-7C7A0F2B3D10",
            "1b4b1a480e194a5c9f5e7c7a0f2b3d10",
        ] {
            assert!(is_uuid(raw), "{raw}");
        }
    }

    #[test]
    fn should_reject_anything_that_is_not_a_uuid() {
        for raw in [
            "",
            "stats",
            "leaderboard",
            "defaults",
            "1b4b1a48-0e19-4a5c-9f5e-7c7a0f2b3d1",
            "1b4b1a48-0e19-4a5c-9f5e-7c7a0f2b3d100",
            "zb4b1a48-0e19-4a5c-9f5e-7c7a0f2b3d10",
            "1b4b1a48-0e19-4a5c-9f5e-7c7a0f2b3d10%20",
            "..",
            "%2e%2e",
            "1b4b1a48-0e19-4a5c-9f5e-7c7a0f2b3d10%2F..",
        ] {
            assert!(!is_uuid(raw), "{raw:?}");
        }
    }

    #[test]
    fn should_accept_real_addresses() {
        for raw in [
            // Wrapped SOL, USDC, a pump.fun launch, the system program.
            "So11111111111111111111111111111111111111112",
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr",
            "11111111111111111111111111111111",
        ] {
            assert!(is_base58_address(raw), "{raw}");
        }
    }

    #[test]
    fn should_reject_anything_that_is_not_a_32_byte_base58_key() {
        for raw in [
            "",
            "short",
            // Outside the base58 alphabet.
            "0OIl0OIl0OIl0OIl0OIl0OIl0OIl0OIl",
            "So1111111111111111111111111111111111111111/",
            "So11111111111111111111111111111111111111%32",
            // Right length band, wrong decoded size.
            &"1".repeat(31),
            &"1".repeat(44),
            &"z".repeat(44),
            // Too long.
            "So11111111111111111111111111111111111111112xy",
            "..",
        ] {
            assert!(!is_base58_address(raw), "{raw:?}");
        }
    }

    #[test]
    fn should_accept_evm_addresses_in_any_case() {
        for raw in [
            "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984",
            "0x1F9840a85d5aF5bf1D1762F925BDADdC4201F984",
            "0x0000000000000000000000000000000000000000",
        ] {
            assert!(is_evm_address(raw), "{raw}");
        }
    }

    #[test]
    fn should_reject_anything_that_is_not_an_evm_address() {
        for raw in [
            "",
            "0x",
            "1f9840a85d5af5bf1d1762f925bdaddc4201f984",
            "0X1f9840a85d5af5bf1d1762f925bdaddc4201f984",
            "0x1f9840a85d5af5bf1d1762f925bdaddc4201f98",
            "0x1f9840a85d5af5bf1d1762f925bdaddc4201f9840",
            "0x1f9840a85d5af5bf1d1762f925bdaddc4201f98g",
            "0x1f9840a85d5af5bf1d1762f925bdaddc4201f9%34",
            "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984%2F..",
            " 0x1f9840a85d5af5bf1d1762f925bdaddc4201f984",
            "So11111111111111111111111111111111111111112",
            "..",
            "%2e%2e",
        ] {
            assert!(!is_evm_address(raw), "{raw:?}");
        }
    }

    #[test]
    fn should_accept_season_numbers_and_nothing_else() {
        for raw in ["1", "6", "2147483647"] {
            assert!(is_season_number(raw), "{raw}");
        }
        for raw in ["", "six", "1.5", "2147483648", "%36", "6/..", " 6"] {
            assert!(!is_season_number(raw), "{raw:?}");
        }
    }

    #[test]
    fn should_fail_closed_on_a_parameter_without_a_check() {
        let error = path_param("chain", "robinhood").expect_err("unknown name");
        assert!(matches!(
            error,
            GatewayError::InvalidPathParam { name: "chain", .. }
        ));
        assert!(!is_known("chain"));
        assert!(path_param("mint", "So11111111111111111111111111111111111111112").is_err());
        assert!(!is_known("mint"));
    }

    #[test]
    fn should_check_a_funding_id_as_a_uuid() {
        for raw in [
            "0f8d2c4e-6a1b-4c3d-9e5f-7a8b9c0d1e2f",
            "0F8D2C4E-6A1B-4C3D-9E5F-7A8B9C0D1E2F",
            "0f8d2c4e6a1b4c3d9e5f7a8b9c0d1e2f",
        ] {
            assert!(path_param("funding_id", raw).is_ok(), "{raw}");
        }
        for raw in [
            "",
            "funding",
            "42",
            "So11111111111111111111111111111111111111112",
            "0f8d2c4e-6a1b-4c3d-9e5f-7a8b9c0d1e2",
            "0f8d2c4e-6a1b-4c3d-9e5f-7a8b9c0d1e2f%2F..",
            "..",
            "%2e%2e",
        ] {
            let error = path_param("funding_id", raw).expect_err(raw);
            assert!(
                matches!(
                    error,
                    GatewayError::InvalidPathParam {
                        name: "funding_id",
                        expected: "a UUID"
                    }
                ),
                "{raw:?}"
            );
        }
    }

    #[test]
    fn should_check_each_known_parameter_with_its_own_rule() {
        assert!(path_param("id", "1b4b1a48-0e19-4a5c-9f5e-7c7a0f2b3d10").is_ok());
        assert!(path_param("id", "So11111111111111111111111111111111111111112").is_err());
        assert!(path_param("number", "6").is_ok());
        assert!(path_param("number", "1b4b1a48").is_err());
        assert!(path_param("address", "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM").is_ok());
        assert!(path_param("address", "11111111111111111111111111111111").is_ok());
        assert!(path_param("address", "1b4b1a48-0e19-4a5c-9f5e-7c7a0f2b3d10").is_err());
        assert!(path_param("address", "0OIl0OIl0OIl0OIl0OIl0OIl0OIl0OIl").is_err());
        assert!(path_param("address", "..").is_err());
        assert!(path_param("address", &"1".repeat(44)).is_err());
        assert!(path_param("token", "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984").is_ok());
        assert!(path_param("wallet", "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984").is_ok());
        assert!(path_param("token", "So11111111111111111111111111111111111111112").is_err());
        assert!(path_param("wallet", "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM").is_err());
        for name in ["id", "funding_id", "number", "address", "token", "wallet"] {
            assert!(is_known(name), "{name}");
        }
    }
}
