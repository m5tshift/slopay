// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Test} from "forge-std/Test.sol";
import {TestUSDT} from "../src/TestUSDT.sol";

contract TestUSDTTest is Test {
    event Transfer(address indexed from, address indexed to, uint256 value);
    event Approval(address indexed owner, address indexed spender, uint256 value);

    TestUSDT token;
    address alice = makeAddr("alice");
    address bob = makeAddr("bob");

    function setUp() public {
        token = new TestUSDT();
    }

    function test_Metadata() public view {
        assertEq(token.decimals(), 6);
        assertEq(token.symbol(), "USDT");
        assertEq(token.totalSupply(), 0);
    }

    function test_MintEmitsTransferFromZero() public {
        vm.expectEmit(true, true, false, true);
        emit Transfer(address(0), alice, 5e6);
        token.mint(alice, 5e6);
        assertEq(token.balanceOf(alice), 5e6);
        assertEq(token.totalSupply(), 5e6);
    }

    function test_MintToZeroReverts() public {
        vm.expectRevert(TestUSDT.ZeroAddress.selector);
        token.mint(address(0), 1);
    }

    function test_TransferEmitsEvent() public {
        token.mint(alice, 10e6);
        vm.expectEmit(true, true, false, true);
        emit Transfer(alice, bob, 4e6);
        vm.prank(alice);
        assertTrue(token.transfer(bob, 4e6));
        assertEq(token.balanceOf(alice), 6e6);
        assertEq(token.balanceOf(bob), 4e6);
    }

    function test_TransferInsufficientReverts() public {
        token.mint(alice, 1);
        vm.expectRevert(abi.encodeWithSelector(TestUSDT.InsufficientBalance.selector, alice, 1, 2));
        vm.prank(alice);
        token.transfer(bob, 2);
    }

    function test_TransferToZeroReverts() public {
        token.mint(alice, 1);
        vm.expectRevert(TestUSDT.ZeroAddress.selector);
        vm.prank(alice);
        token.transfer(address(0), 1);
    }

    function test_ApproveAndTransferFrom() public {
        token.mint(alice, 10e6);
        vm.expectEmit(true, true, false, true);
        emit Approval(alice, bob, 3e6);
        vm.prank(alice);
        token.approve(bob, 3e6);

        vm.prank(bob);
        token.transferFrom(alice, bob, 2e6);
        assertEq(token.allowance(alice, bob), 1e6);
        assertEq(token.balanceOf(bob), 2e6);
    }

    function test_TransferFromOverAllowanceReverts() public {
        token.mint(alice, 10e6);
        vm.prank(alice);
        token.approve(bob, 1e6);
        vm.expectRevert(abi.encodeWithSelector(TestUSDT.InsufficientAllowance.selector, bob, 1e6, 2e6));
        vm.prank(bob);
        token.transferFrom(alice, bob, 2e6);
    }

    function test_InfiniteAllowanceNotDecreased() public {
        token.mint(alice, 10e6);
        vm.prank(alice);
        token.approve(bob, type(uint256).max);
        vm.prank(bob);
        token.transferFrom(alice, bob, 2e6);
        assertEq(token.allowance(alice, bob), type(uint256).max);
    }

    function testFuzz_TransferConservesSupply(uint96 minted, uint96 sent) public {
        token.mint(alice, minted);
        sent = uint96(bound(sent, 0, minted));
        vm.prank(alice);
        token.transfer(bob, sent);
        assertEq(token.balanceOf(alice) + token.balanceOf(bob), token.totalSupply());
        assertEq(token.balanceOf(bob), sent);
    }
}
