'use strict';

var node = require('../../../node');
var shared = require('./shared');
var constants = require('../../../../helpers/constants');

var sendTransactionPromise = require('../../../common/complexTransactions').sendTransactionPromise;
var creditAccountPromise = require('../../../common/complexTransactions').creditAccountPromise;
var onNewBlockPromise = node.Promise.promisify(node.onNewBlock);

describe('POST /api/transactions (type 2) register delegate', function () {

	var badTransactions = [];
	var goodTransactions = [];
	var badTransactionsEnforcement = [];
	var goodTransactionsEnforcement = [];

	var account = node.randomAccount();
	var accountNoFunds = node.randomAccount();
	var accountScarceFunds = node.randomAccount();

	var transaction;

	// Crediting accounts
	before(function () {

		var promises = [];
		promises.push(creditAccountPromise(account.address, 100000000000));
		promises.push(creditAccountPromise(accountScarceFunds.address, constants.fees.dapp));

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

		shared.invalidAssets(account, 'dapp', badTransactions);

		it('without type should fail', function () {
			transaction = node.lisk.dapp.createDapp(account.password, null, node.guestbookDapp);
			delete transaction.asset.dapp.type;

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.be.not.ok;
				node.expect(res).to.have.property('message').to.match(/Missing required property: type$/);
				badTransactions.push(transaction);
			});
		});

		it('without name should fail', function () {
			transaction = node.lisk.dapp.createDapp(account.password, null, node.guestbookDapp);
			delete transaction.asset.dapp.name;

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.be.not.ok;
				node.expect(res).to.have.property('message').to.match(/Missing required property: name$/);
				badTransactions.push(transaction);
			});
		});

		it('without category should fail', function () {
			transaction = node.lisk.dapp.createDapp(account.password, null, node.guestbookDapp);
			delete transaction.asset.dapp.category;

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.be.not.ok;
				node.expect(res).to.have.property('message').to.match(/Missing required property: category$/);
				badTransactions.push(transaction);
			});
		});

		it('with negative type should fail', function () {
			transaction = node.lisk.dapp.createDapp(account.password, null, node.guestbookDapp);
			transaction.asset.dapp.type = -1;

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.be.not.ok;
				node.expect(res).to.have.property('message').to.match(/Value -1 is less than minimum 0$/);
				badTransactions.push(transaction);
			});
		});

		it('with type smaller than minimum should fail', function () {
			transaction = node.lisk.dapp.createDapp(account.password, null, node.guestbookDapp);
			transaction.asset.dapp.type = -1;

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.be.not.ok;
				node.expect(res).to.have.property('message').to.match(/Value -1 is less than minimum 0$/);
				badTransactions.push(transaction);
			});
		});

		it('with type greater than maximum should fail', function () {
			var application = node.randomApplication();
			application.type = 2;
			transaction = node.lisk.dapp.createDapp(account.password, null, application);

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.be.not.ok;
				node.expect(res).to.have.property('message').to.equal('Invalid application type');
				badTransactions.push(transaction);
			});
		});

		it('with category less than minimum should fail', function () {
			transaction = node.lisk.dapp.createDapp(account.password, null, node.guestbookDapp);
			transaction.asset.dapp.category = -1;

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.be.not.ok;
				node.expect(res).to.have.property('message').to.match(/Value -1 is less than minimum 0$/);
				badTransactions.push(transaction);
			});
		});

		it('with category greater than maximum should fail', function () {
			transaction = node.lisk.dapp.createDapp(account.password, null, node.guestbookDapp);
			transaction.asset.dapp.category = 9;

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.be.not.ok;
				node.expect(res).to.have.property('message').to.match(/Value 9 is greater than maximum 8$/);
				badTransactions.push(transaction);
			});
		});

		it('with empty name should fail', function () {
			transaction = node.lisk.dapp.createDapp(account.password, null, node.guestbookDapp);
			transaction.asset.dapp.name = '';

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.be.not.ok;
				node.expect(res).to.have.property('message').to.match(/String is too short \(0 chars\), minimum 1$/);
				badTransactions.push(transaction);
			});
		});

		it('with name longer than maximum should fail', function () {
			transaction = node.lisk.dapp.createDapp(account.password, null, node.guestbookDapp);
			transaction.asset.dapp.name = node.randomApplicationName() + 'A';

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.be.not.ok;
				node.expect(res).to.have.property('message').to.match(/String is too long \(33 chars\), maximum 32$/);
				badTransactions.push(transaction);
			});
		});
	});

	describe('transactions processing', function () {

		it('with no funds should fail', function () {
			transaction = node.lisk.dapp.createDapp(accountNoFunds.password, null, node.randomApplication());

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.not.be.ok;
				node.expect(res).to.have.property('message').to.equal('Account does not have enough LSK: ' + accountNoFunds.address + ' balance: 0');
				badTransactions.push(transaction);
			});
		});

		it('with exact funds should be ok', function () {
			transaction = node.lisk.dapp.createDapp(accountScarceFunds.password, null, node.randomApplication());

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.be.ok;
				node.expect(res).to.have.property('transactionId').to.equal(transaction.id);
				goodTransactions.push(transaction);
			});
		});

		it('with valid params should be ok', function () {
			transaction = node.lisk.dapp.createDapp(account.password, null, node.randomApplication());

			return sendTransactionPromise(transaction).then(function (res) {
				node.expect(res).to.have.property('success').to.be.ok;
				node.expect(res).to.have.property('transactionId').to.equal(transaction.id);
				goodTransactions.push(transaction);
			});
		});
	});

	describe('transactions confirmation', function () {

		shared.confirmationPhase(goodTransactions, badTransactions);
	});
});
