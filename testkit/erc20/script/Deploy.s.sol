// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Script, console} from "forge-std/Script.sol";
import {TestUSDT} from "../src/TestUSDT.sol";

/// Deploys TestUSDT and mints MINT_AMOUNT minimal units to every address in the
/// comma-separated MINT_TO list (both optional).
contract Deploy is Script {
    function run() external returns (TestUSDT token) {
        address[] memory recipients = vm.envOr("MINT_TO", ",", new address[](0));
        uint256 amount = vm.envOr("MINT_AMOUNT", uint256(1_000_000e6));

        vm.startBroadcast();
        token = new TestUSDT();
        for (uint256 i = 0; i < recipients.length; i++) {
            token.mint(recipients[i], amount);
        }
        vm.stopBroadcast();

        console.log("TestUSDT deployed at", address(token));
    }
}
