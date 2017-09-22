'use strict';

var node = require('../../../node');
var shared = require('./shared');
var constants = require('../../../../helpers/constants');

var sendTransaction = require('../../../common/complexTransactions').sendTransaction;
var creditAccount = require('../../../common/complexTransactions').creditAccount;

var sendTransactionPromise = node.Promise.promisify(sendTransaction);
var creditAccountPromise = node.Promise.promisify(creditAccount);
var onNewBlockPromise = node.Promise.promisify(node.onNewBlock);

describe('POST /api/transactions (type 2) register delegate', function () {

	var badTransactions = [];
	var goodTransactions = [];
	var badTransactionsEnforcement = [];
	var goodTransactionsEnforcement = [];

	var account = node.randomAccount();
	var accountNoFunds = node.randomAccount();
	var accountScarceFunds = node.randomAccount();
	var accountUpperCase = node.randomAccount();

	var transaction;

	// Crediting accounts
	before(function () {

		var promises = [];
		promises.push(creditAccountPromise(account.address, 100000000000));
		promises.push(creditAccountPromise(accountScarceFunds.address, constants.fees.delegate));
		promises.push(creditAccountPromise(accountUpperCase.address, constants.fees.delegate));

		return node.Promise.all(promises).then(function (results) {
			results.forEach(function (res) {
				node.expect(res).to.have.property('success').to.be.ok;
				node.expect(res).to.have.property('transactionId').that.is.not.empty;
			});
		}).then(function (res) {
			return onNewBlockPromise();
		});
	});

	describe('schema validations', function () {

		shared.invalidAssets(account, 'delegate', badTransactions);
	});

	describe('transactions processing', function () {

		it('with no funds should fail', function () {
			accountNoFunds = node.randomAccount();
			transaction = node.lisk.delegate.createDelegate(accountNoFunds.password, accountNoFunds.username);

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.not.be.ok;
				node.expect(res).to.have.property('message').to.equal('Account does not have enough LSK: ' + accountNoFunds.address + ' balance: 0');
				badTransactions.push(transaction);
			});
		});

		it('with exact funds should be ok', function () {
			transaction = node.lisk.delegate.createDelegate(accountScarceFunds.password, accountScarceFunds.username);

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.be.ok;
				node.expect(res).to.have.property('transactionId').to.equal(transaction.id);
				goodTransactions.push(transaction);
			});
		});

		it('with uppercase username should fail', function () {
			transaction = node.lisk.delegate.createDelegate(accountUpperCase.password, accountUpperCase.username.toUpperCase());

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.be.not.ok;
				node.expect(res).to.have.property('message').to.equal('Username must be lowercase');
				badTransactions.push(transaction);
			});
		});

		it('with valid params should be ok', function () {
			transaction = node.lisk.delegate.createDelegate(account.password, account.username);

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.be.ok;
				node.expect(res).to.have.property('transactionId').to.equal(transaction.id);
				goodTransactions.push(transaction);
			});
		});

		// TODO: bug #729
		// it('setting delegate twice with valid params should be ok', function () {
		// 	transaction = node.lisk.delegate.createDelegate(account.password, node.randomDelegateName());
		// 	return sendTransactionPromise(transaction).then(function (res) {
		// 		node.expect(res).to.have.property('success').to.be.ok;
		// 		node.expect(res).to.have.property('transactionId').to.equal(transaction.id);
		// 		goodTransactions.push(transaction);
		// 	});
		// });
	});

	describe('transactions confirmation', function () {

		shared.confirmationPhase(goodTransactions, badTransactions);
	});

	describe('enforcement', function () {

		it('setting same delegate twice should fail', function () {
			transaction = node.lisk.delegate.createDelegate(account.password, account.username);

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.not.be.ok;
				node.expect(res).to.have.property('message').to.equal('Account is already a delegate');
				badTransactionsEnforcement.push(transaction);
			});
		});

		it('updating registered delegate should fail', function () {
			transaction = node.lisk.delegate.createDelegate(account.password, 'newusername');

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.not.be.ok;
				node.expect(res).to.have.property('message').to.equal('Account is already a delegate');
				badTransactionsEnforcement.push(transaction);
			});
		});
	});

	describe('enforcement confirmation', function () {

		shared.confirmationPhase(goodTransactionsEnforcement, badTransactionsEnforcement);
	});
});
