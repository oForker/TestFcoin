'use strict';

var node = require('../../../node');
var shared = require('./shared');
var constants = require('../../../../helpers/constants');

var sendTransactionPromise = require('../../../common/complexTransactions').sendTransactionPromise;
var creditAccountPromise = require('../../../common/complexTransactions').creditAccountPromise;
var sendSignaturePromise = require('../../../common/complexTransactions').sendSignaturePromise;
var onNewBlockPromise = node.Promise.promisify(node.onNewBlock);

function Scenario (size, amount) {
	this.account = node.randomAccount();
	this.members = new Array();
	this.keysgroup = new Array();

	var i, auxAccount;
	for(i = 0; i < size-1; i++) {
	 auxAccount = node.randomAccount();
	 this.members.push(auxAccount);
	 this.keysgroup.push('+' + auxAccount.publicKey);
	}

	this.tx = null;
	this.amount = amount || 100000000000;
}

describe('POST /api/transactions (type 4) register multisignature', function () {

	var scenarios = {
		'no_funds': 								new Scenario(3, 0),
		'scarce_funds': 						new Scenario(3, constants.fees.multisignature * 3),
		'minimum_not_reached':			new Scenario(4), //4_2
		'regular':									new Scenario(3), //3_2
		'max_signatures': 					new Scenario(16), //16_2
		'max_signatures_max_min': 	new Scenario(16), //16_16
		'more_than_max_signatures': new Scenario(18), //18_2
	};

	var transaction, signature;

	var badTransactions = [];
	var goodTransactions = [];
	var badTransactionsEnforcement = [];
	var goodTransactionsEnforcement = [];
	var pendingMultisignatures = [];

	before(function () {
		//Crediting accounts
		return node.Promise.all(Object.keys(scenarios).map(function (key) {

			if(key === 'no_funds') {return;}

			return creditAccountPromise(scenarios[key].account.address, scenarios[key].amount).then(function (res) {
				node.expect(res).to.have.property('success').to.be.ok;
				node.expect(res).to.have.property('transactionId').that.is.not.empty;
			});
		})).then(function (res) {
			return onNewBlockPromise();
		});
	});

	describe('schema validations', function () {

		shared.invalidAssets(scenarios.regular.account, 'multisignature', badTransactions);

		it('using empty keysgroup should fail', function () {
			transaction = node.lisk.multisignature.createMultisignature(scenarios.regular.account.password, null, [], 1, 2);

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.be.not.ok;
				node.expect(res).to.have.property('message').to.equal('Invalid transaction body - Failed to validate multisignature schema: Array is too short (0), minimum 1');
				badTransactions.push(transaction);
			});
		});

		it('using sender in the keysgroup should fail', function () {
			transaction = node.lisk.multisignature.createMultisignature(scenarios.regular.account.password, null, ['+' + node.eAccount.publicKey, '+' + scenarios.regular.account.publicKey], 1, 2);

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.be.not.ok;
				node.expect(res).to.have.property('message').to.equal('Invalid multisignature keysgroup. Can not contain sender');
				badTransactions.push(transaction);
			});
		});

		it('using no math operator in keysgroup should fail', function () {
			transaction = node.lisk.multisignature.createMultisignature(scenarios.regular.account.password, null, [node.eAccount.publicKey, scenarios.no_funds.account.publicKey, scenarios.scarce_funds.account.publicKey], 1, 2);

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.be.not.ok;
				node.expect(res).to.have.property('message').to.equal('Invalid math operator in multisignature keysgroup');
				badTransactions.push(transaction);
			});
		});

		it('just math operator in keysgroup should fail', function () {
			transaction = node.lisk.multisignature.createMultisignature(scenarios.regular.account.password, null, ['+','+'], 1, 2);

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.be.not.ok;
				node.expect(res).to.have.property('message').to.equal('Invalid public key in multisignature keysgroup');
				badTransactions.push(transaction);
			});
		});

		it('using invalid math operator in keysgroup should fail', function () {
			transaction = node.lisk.multisignature.createMultisignature(scenarios.regular.account.password, null, ['-' + node.eAccount.publicKey, '+' + scenarios.no_funds.account.publicKey], 1, 2);

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.be.not.ok;
				node.expect(res).to.have.property('message').to.equal('Invalid math operator in multisignature keysgroup');
				badTransactions.push(transaction);
			});
		});

		it('using same member twice should fail', function () {
			transaction = node.lisk.multisignature.createMultisignature(scenarios.regular.account.password, null, ['+' + node.eAccount.publicKey, '+' + node.eAccount.publicKey], 1, 2);

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.be.not.ok;
				node.expect(res).to.have.property('message').to.equal('Encountered duplicate public key in multisignature keysgroup');
				badTransactions.push(transaction);
			});
		});

		// TODO: bug in 1.0.0 waiting for backport 0.9.7
		it.skip('using empty member in keysgroup should fail', function () {
			transaction = node.lisk.multisignature.createMultisignature(scenarios.regular.account.password, null, ['+' + node.eAccount.publicKey, '+' + scenarios.no_funds.account.publicKey, '+' + scenarios.scarce_funds.account.publicKey, null], 1, 2);

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.be.not.ok;
				node.expect(res).to.have.property('message').to.equal('Invalid math operator in multisignature keysgroup');
				badTransactions.push(transaction);
			});
		});

		// TODO: change sentence 'Must be less than or equal to keysgroup size + 1'
		it('using min bigger than keysgroup size plus 1 should fail', function () {
			transaction = node.lisk.multisignature.createMultisignature(scenarios.regular.account.password, null, [node.eAccount.publicKey], 1, 2);

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.be.not.ok;
				node.expect(res).to.have.property('message').to.equal('Invalid multisignature min. Must be less than keysgroup size');
				badTransactions.push(transaction);
			});
		});

		it('using min more than maximum(15) should fail', function () {
			transaction = node.lisk.multisignature.createMultisignature(scenarios.max_signatures_max_min.account.password, null, scenarios.max_signatures_max_min.keysgroup, 1, 16);

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.be.not.ok;
				node.expect(res).to.have.property('message').to.equal('Invalid transaction body - Failed to validate multisignature schema: Value 16 is greater than maximum 15');
				badTransactions.push(transaction);
			});
		});

		// TODO: Error message change : Must be between 2 and 16
		it('using min less than minimum(2) should fail', function () {
			transaction = node.lisk.multisignature.createMultisignature(scenarios.max_signatures.account.password, null, scenarios.max_signatures.keysgroup, 1, 1);

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.be.not.ok;
				node.expect(res).to.have.property('message').to.equal('Invalid multisignature min. Must be between 1 and 16');
				badTransactions.push(transaction);
			});
		});
	});

	describe('transactions processing', function () {

		it('with no_funds scenario should fail', function () {
			transaction = node.lisk.multisignature.createMultisignature(scenarios.no_funds.account.password, null, scenarios.no_funds.keysgroup, 1, 2);

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.not.be.ok;
				node.expect(res).to.have.property('message').to.equal('Account does not have enough LSK: ' + scenarios.no_funds.account.address + ' balance: 0');
				badTransactions.push(transaction);
			});
		});

		it('with scarce_funds scenario should be ok', function () {
			transaction = node.lisk.multisignature.createMultisignature(scenarios.scarce_funds.account.password, null, scenarios.scarce_funds.keysgroup, 1, 2);

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.be.ok;
				node.expect(res).to.have.property('transactionId').to.equal(transaction.id);
				scenarios.scarce_funds.tx = transaction;
			});
		});

		it('using valid params regular scenario (3,2) should be ok', function () {
			transaction = node.lisk.multisignature.createMultisignature(scenarios.regular.account.password, null, scenarios.regular.keysgroup, 1, 2);

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.be.ok;
				node.expect(res).to.have.property('transactionId').to.equal(transaction.id);
				scenarios.regular.tx = transaction;
			});
		});

		it('using valid params minimum_not_reached scenario (4,2) should be ok', function () {
			transaction = node.lisk.multisignature.createMultisignature(scenarios.minimum_not_reached.account.password, null, scenarios.minimum_not_reached.keysgroup, 1, 2);

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.be.ok;
				node.expect(res).to.have.property('transactionId').to.equal(transaction.id);
				scenarios.minimum_not_reached.tx = transaction;
			});
		});

		it('using valid params max_signatures scenario (16,2) should be ok', function () {
			transaction = node.lisk.multisignature.createMultisignature(scenarios.max_signatures.account.password, null, scenarios.max_signatures.keysgroup, 1, 2);

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.be.ok;
				node.expect(res).to.have.property('transactionId').to.equal(transaction.id);
				scenarios.max_signatures.tx = transaction;
			});
		});

		it('using valid params max_signatures_max_min scenario (16,16) should be ok', function () {
			transaction = node.lisk.multisignature.createMultisignature(scenarios.max_signatures_max_min.account.password, null, scenarios.max_signatures_max_min.keysgroup, 1, 2);

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.be.ok;
				node.expect(res).to.have.property('transactionId').to.equal(transaction.id);
				scenarios.max_signatures_max_min.tx = transaction;
			});
		});

		it('using more_than_max_signatures scenario (18,2) should fail', function () {
			transaction = node.lisk.multisignature.createMultisignature(scenarios.more_than_max_signatures.account.password, null, scenarios.more_than_max_signatures.keysgroup, 1, 2);

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.be.not.ok;
				node.expect(res).to.have.property('message').to.equal('Invalid transaction body - Failed to validate multisignature schema: Array is too long (17), maximum 16');
				badTransactions.push(transaction);
			});
		});

		describe('signing transactions', function () {

			it('with not all the signatures minimum_not_reached scenario (4,2) should be ok but never confirmed', function () {
				signature = node.lisk.multisignature.signTransaction(scenarios.minimum_not_reached.tx, scenarios.minimum_not_reached.members[0].password);

				return sendSignaturePromise(signature, scenarios.minimum_not_reached.tx).then(function (res) {
					node.expect(res).to.have.property('success').to.be.ok;
					pendingMultisignatures.push(scenarios.minimum_not_reached.tx);
				});
			});

			it('twice with the same account should fail', function () {
				signature = node.lisk.multisignature.signTransaction(scenarios.minimum_not_reached.tx, scenarios.minimum_not_reached.members[0].password);

				return sendSignaturePromise(signature, scenarios.minimum_not_reached.tx).then(function (res) {
					node.expect(res).to.have.property('success').to.be.not.ok;
					node.expect(res).to.have.property('message').to.equal('Error processing signature: Permission to sign transaction denied');
				});
			});

			it('with not requested account should fail', function () {
				signature = node.lisk.multisignature.signTransaction(scenarios.minimum_not_reached.tx, node.randomAccount().password);

				return sendSignaturePromise(signature, scenarios.minimum_not_reached.tx).then(function (res) {
					node.expect(res).to.have.property('success').to.be.not.ok;
					node.expect(res).to.have.property('message').to.equal('Error processing signature: Failed to verify signature');
				});
			});

			it('with all the signatures regular scenario (3,2) should be ok and confirmed', function () {
				return node.Promise.all(node.Promise.map(scenarios.regular.members, function (member) {
					signature = node.lisk.multisignature.signTransaction(scenarios.regular.tx, member.password);

					return sendSignaturePromise(signature, scenarios.regular.tx).then(function (res) {
						node.expect(res).to.have.property('success').to.be.ok;
					});
				})).then(function (res) {
					goodTransactions.push(scenarios.regular.tx);
				});
			});

			it('with all the signatures already in place regular scenario (3,2) should fail', function () {
				signature = node.lisk.multisignature.signTransaction(scenarios.regular.tx, scenarios.regular.members[0].password);

				return sendSignaturePromise(signature, scenarios.regular.tx).then(function (res) {
					node.expect(res).to.have.property('success').to.be.not.ok;
					node.expect(res).to.have.property('message').to.equal('Error processing signature: Permission to sign transaction denied');
				});
			});

			it('with all the signatures max_signatures scenario (16,2) should be ok and confirmed', function () {
				return node.Promise.all(node.Promise.map(scenarios.max_signatures.members, function (member) {
					signature = node.lisk.multisignature.signTransaction(scenarios.max_signatures.tx, member.password);

					return sendSignaturePromise(signature, scenarios.max_signatures.tx).then(function (res) {
						node.expect(res).to.have.property('success').to.be.ok;
					});
				})).then(function (res) {
					goodTransactions.push(scenarios.max_signatures.tx);
				});
			});

			it('with all the signatures max_signatures_max_min scenario (16,16) should be ok and confirmed', function () {
				return node.Promise.all(node.Promise.map(scenarios.max_signatures_max_min.members, function (member) {
					signature = node.lisk.multisignature.signTransaction(scenarios.max_signatures_max_min.tx, member.password);

					return sendSignaturePromise(signature, scenarios.max_signatures_max_min.tx).then(function (res) {
						node.expect(res).to.have.property('success').to.be.ok;
					});
				})).then(function (res) {
					goodTransactions.push(scenarios.max_signatures_max_min.tx);
				});
			});
		});

		// TODO: remove after patch 0.9.8
		it.skip('with 20 signs 2 min BUGG', function () {
			return node.Promise.all(node.Promise.map(scenarios.more_than_max_signatures.members, function (member) {
				signature = node.lisk.multisignature.signTransaction(scenarios.more_than_max_signatures.tx, member.password);

				return sendSignaturePromise(signature, scenarios.more_than_max_signatures.tx).then(function (res) {
					node.expect(res).to.have.property('success').to.be.ok;
				});
			})).then(function (res) {
				goodTransactions.push(scenarios.more_than_max_signatures.tx);
			});
		});
	});

	describe('transactions confirmation', function () {

		shared.confirmationPhase(goodTransactions, badTransactions, pendingMultisignatures);
	});

	describe('enforcement', function () {

		describe('type 0 - sending funds', function () {

			it('minimum_not_reached scenario(4,2) should be ok and confirmed without member signatures', function () {
				transaction = node.lisk.transaction.createTransaction(scenarios.regular.account.address, 1, scenarios.minimum_not_reached.account.password);

				return sendTransactionPromise(transaction).then(function (res) {
					node.expect(res).to.have.property('success').to.be.ok;
					node.expect(res).to.have.property('transactionId').to.equal(transaction.id);
					goodTransactionsEnforcement.push(transaction);
				});
			});

			it('regular scenario(3,2) should be ok', function () {
				transaction = node.lisk.transaction.createTransaction(scenarios.max_signatures.account.address, 1, scenarios.regular.account.password);

				return sendTransactionPromise(transaction).then(function (res) {
					node.expect(res).to.have.property('success').to.be.ok;
					node.expect(res).to.have.property('transactionId').to.equal(transaction.id);
					scenarios.regular.tx = transaction;
				});
			});

			it('max_signatures scenario(16,2) should be ok but never confirmed without the minimum signatures', function () {
				transaction = node.lisk.transaction.createTransaction(scenarios.regular.account.address, 1, scenarios.max_signatures.account.password);

				return sendTransactionPromise(transaction).then(function (res) {
					node.expect(res).to.have.property('success').to.be.ok;
					node.expect(res).to.have.property('transactionId').to.equal(transaction.id);
					pendingMultisignatures.push(transaction);
				});
			});

			it('max_signatures_max_min scenario(16,16) should be ok', function () {
				transaction = node.lisk.transaction.createTransaction(scenarios.regular.account.address, 1, scenarios.max_signatures_max_min.account.password);

				return sendTransactionPromise(transaction).then(function (res) {
					node.expect(res).to.have.property('success').to.be.ok;
					node.expect(res).to.have.property('transactionId').to.equal(transaction.id);
					scenarios.max_signatures_max_min.tx = transaction;
				});
			});

			describe('signing transactions', function () {

				it('with min required signatures regular scenario(3,2) should be ok and confirmed', function () {
					return node.Promise.all(node.Promise.map(scenarios.regular.members, function (member) {
						signature = node.lisk.multisignature.signTransaction(scenarios.regular.tx, member.password);

						return sendSignaturePromise(signature, scenarios.regular.tx).then(function (res) {
							node.expect(res).to.have.property('success').to.be.ok;
						});
					})).then(function (res) {
						goodTransactionsEnforcement.push(scenarios.regular.tx);
					});
				});

				it('with min required signatures max_signatures_max_min scenario(16,16) should be ok and confirmed', function () {
					return node.Promise.all(node.Promise.map(scenarios.max_signatures_max_min.members, function (member) {
						signature = node.lisk.multisignature.signTransaction(scenarios.max_signatures_max_min.tx, member.password);

						return sendSignaturePromise(signature, scenarios.max_signatures_max_min.tx).then(function (res) {
							node.expect(res).to.have.property('success').to.be.ok;
						});
					})).then(function (res) {
						goodTransactionsEnforcement.push(scenarios.max_signatures_max_min.tx);
					});
				});
			});
		});

		describe('type 1 - second secret', function () {

			it('regular scenario(3,2) should be ok', function () {
				transaction = node.lisk.signature.createSignature(scenarios.regular.account.password, scenarios.regular.account.secondPassword);

				return sendTransactionPromise(transaction).then(function (res) {
					node.expect(res).to.have.property('success').to.be.ok;
					node.expect(res).to.have.property('transactionId').to.equal(transaction.id);
					scenarios.regular.tx = transaction;
				});
			});

			describe('signing transactions', function () {

				it('with min required signatures regular scenario(3,2) should be ok and confirmed', function () {
					return node.Promise.all(node.Promise.map(scenarios.regular.members, function (member) {
						signature = node.lisk.multisignature.signTransaction(scenarios.regular.tx, member.password);

						return sendSignaturePromise(signature, scenarios.regular.tx).then(function (res) {
							node.expect(res).to.have.property('success').to.be.ok;
						});
					})).then(function (res) {
						goodTransactionsEnforcement.push(scenarios.regular.tx);
					});
				});
			});
		});

		describe('type 2 - registering delegate', function () {

			it('regular scenario(3,2) should be ok', function () {
				transaction = node.lisk.delegate.createDelegate(scenarios.regular.account.password, scenarios.regular.account.username);

				return sendTransactionPromise(transaction).then(function (res) {
					node.expect(res).to.have.property('success').to.be.ok;
					node.expect(res).to.have.property('transactionId').to.equal(transaction.id);
					scenarios.regular.tx = transaction;
				});
			});

			describe('signing transactions', function () {

				it('with min required signatures regular scenario(3,2) should be ok and confirmed', function () {
					return node.Promise.all(node.Promise.map(scenarios.regular.members, function (member) {
						signature = node.lisk.multisignature.signTransaction(scenarios.regular.tx, member.password);

						return sendSignaturePromise(signature, scenarios.regular.tx).then(function (res) {
							node.expect(res).to.have.property('success').to.be.ok;
						});
					})).then(function (res) {
						goodTransactionsEnforcement.push(scenarios.regular.tx);
					});
				});
			});
		});

		describe('type 3 - voting delegate', function () {

			it('regular scenario(3,2) should be ok', function () {
				transaction = node.lisk.vote.createVote(scenarios.regular.account.password, ['+' + node.eAccount.publicKey]);

				return sendTransactionPromise(transaction).then(function (res) {
					node.expect(res).to.have.property('success').to.be.ok;
					node.expect(res).to.have.property('transactionId').to.equal(transaction.id);
					scenarios.regular.tx = transaction;
				});
			});

			describe('signing transactions', function () {

				it('with min required signatures regular scenario(3,2) should be ok and confirmed', function () {
					return node.Promise.all(node.Promise.map(scenarios.regular.members, function (member) {
						signature = node.lisk.multisignature.signTransaction(scenarios.regular.tx, member.password);

						return sendSignaturePromise(signature, scenarios.regular.tx).then(function (res) {
							node.expect(res).to.have.property('success').to.be.ok;
						});
					})).then(function (res) {
						goodTransactionsEnforcement.push(scenarios.regular.tx);
					});
				});
			});
		});

		describe('type 4 - registering multisignature account', function () {

			it('with an account already registered should fail', function () {
				transaction = node.lisk.multisignature.createMultisignature(scenarios.regular.account.password, null, scenarios.regular.keysgroup, 1, 2);

				return sendTransactionPromise(transaction).then(function (res) {
					node.expect(res).to.have.property('success').to.be.not.ok;
					node.expect(res).to.have.property('message').to.equal('Account already has multisignatures enabled');
					badTransactionsEnforcement.push(transaction);
				});
			});
		});

		describe('type 5 - registering dapp', function () {

			it('regular scenario(3,2) should be ok', function () {
				transaction = node.lisk.dapp.createDapp(scenarios.regular.account.password, null, node.randomApplication());

				return sendTransactionPromise(transaction).then(function (res) {
					node.expect(res).to.have.property('success').to.be.ok;
					node.expect(res).to.have.property('transactionId').to.equal(transaction.id);
					scenarios.regular.tx = transaction;
				});
			});

			describe('signing transactions', function () {

				it('with min required signatures regular scenario(3,2) should be ok and confirmed', function () {
					return node.Promise.all(node.Promise.map(scenarios.regular.members, function (member) {
						signature = node.lisk.multisignature.signTransaction(scenarios.regular.tx, member.password);

						return sendSignaturePromise(signature, scenarios.regular.tx).then(function (res) {
							node.expect(res).to.have.property('success').to.be.ok;
						});
					})).then(function (res) {
						goodTransactionsEnforcement.push(scenarios.regular.tx);
					});
				});
			});
		});
	});

	describe('enforcement confirmation', function () {

		shared.confirmationPhase(goodTransactionsEnforcement, badTransactionsEnforcement, pendingMultisignatures);
	});
});
