/*
 * Copyright (C) 2020 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.projectnessie.jaxrs;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.projectnessie.model.Validation.HASH_MESSAGE;
import static org.projectnessie.model.Validation.REF_NAME_MESSAGE;
import static org.projectnessie.model.Validation.REF_NAME_OR_HASH_MESSAGE;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.client.StreamingUtil;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.api.NessieApiVersion;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.client.rest.NessieBadRequestException;
import org.projectnessie.client.rest.NessieHttpResponseFilter;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.model.Contents.Type;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableDeltaLakeTable;
import org.projectnessie.model.ImmutableSqlView;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Operation.Unchanged;
import org.projectnessie.model.Reference;
import org.projectnessie.model.SqlView;
import org.projectnessie.model.SqlView.Dialect;
import org.projectnessie.model.Tag;

public abstract class AbstractTestRest {
  public static final String COMMA_VALID_HASH_1 =
      ",1234567890123456789012345678901234567890123456789012345678901234";
  public static final String COMMA_VALID_HASH_2 = ",1234567890123456789012345678901234567890";
  public static final String COMMA_VALID_HASH_3 = ",1234567890123456";

  private NessieApiV1 api;
  private HttpClient httpClient;

  static {
    // Note: REST tests validate some locale-specific error messages, but expect on the messages to
    // be in ENGLISH. However, the JRE's startup classes (in particular class loaders) may cause the
    // default Locale to be initialized before Maven is able to override the user.language system
    // property. Therefore, we explicitly set the default Locale to ENGLISH here to match tests'
    // expectations.
    Locale.setDefault(Locale.ENGLISH);
  }

  protected void init(URI uri) {
    NessieApiV1 api =
        HttpClientBuilder.builder().withUri(uri).build(NessieApiVersion.V_1, NessieApiV1.class);

    ObjectMapper mapper =
        new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    HttpClient httpClient = HttpClient.builder().setBaseUri(uri).setObjectMapper(mapper).build();
    httpClient.register(new NessieHttpResponseFilter(mapper));

    init(api, httpClient);
  }

  protected void init(NessieApiV1 api, HttpClient httpClient) {
    this.api = api;
    this.httpClient = httpClient;
  }

  @BeforeEach
  public void setUp() throws Exception {}

  @AfterEach
  public void tearDown() {
    api.close();
  }

  @Test
  void createReferences() throws NessieNotFoundException {
    String mainHash = api.getReference().refName("main").submit().getHash();

    String tagName1 = "createReferences_tag1";
    String tagName2 = "createReferences_tag2";
    String branchName1 = "createReferences_branch1";
    String branchName2 = "createReferences_branch2";
    assertAll(
        // Tag without hash
        () ->
            assertThatThrownBy(
                    () ->
                        api.createReference()
                            .sourceRefName("main")
                            .reference(Tag.of(tagName1, null))
                            .submit())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("Tag-creation requires a target named-reference and hash."),
        // legit Tag with name + hash
        () -> {
          Reference refTag1 =
              api.createReference()
                  .sourceRefName("main")
                  .reference(Tag.of(tagName2, mainHash))
                  .submit();
          assertEquals(Tag.of(tagName2, mainHash), refTag1);
        },
        // Branch without hash
        () -> {
          Reference refBranch1 =
              api.createReference()
                  .sourceRefName("main")
                  .reference(Branch.of(branchName1, null))
                  .submit();
          assertEquals(Branch.of(branchName1, mainHash), refBranch1);
        },
        // Branch with name + hash
        () -> {
          Reference refBranch2 =
              api.createReference()
                  .sourceRefName("main")
                  .reference(Branch.of(branchName2, mainHash))
                  .submit();
          assertEquals(Branch.of(branchName2, mainHash), refBranch2);
        });
  }

  @ParameterizedTest
  @ValueSource(strings = {"normal", "with-no_space", "slash/thing"})
  void referenceNames(String refNamePart) throws NessieNotFoundException, NessieConflictException {
    String tagName = "tag" + refNamePart;
    String branchName = "branch" + refNamePart;
    String branchName2 = "branch2" + refNamePart;

    String root = "ref_name_" + refNamePart.replaceAll("[^a-z]", "");
    Reference main =
        api.createReference().sourceRefName("main").reference(Branch.of(root, null)).submit();

    IcebergTable meta = IcebergTable.of("meep", -1L);
    main =
        api.commitMultipleOperations()
            .branchName(main.getName())
            .hash(main.getHash())
            .commitMeta(
                CommitMeta.builder()
                    .message("common-merge-ancestor")
                    .properties(ImmutableMap.of("prop1", "val1", "prop2", "val2"))
                    .build())
            .operation(Operation.Put.of(ContentsKey.of("meep"), meta))
            .submit();
    String someHash = main.getHash();

    Reference createdTag =
        api.createReference()
            .sourceRefName(main.getName())
            .reference(Tag.of(tagName, someHash))
            .submit();
    assertEquals(Tag.of(tagName, someHash), createdTag);
    Reference createdBranch1 =
        api.createReference()
            .sourceRefName(main.getName())
            .reference(Branch.of(branchName, someHash))
            .submit();
    assertEquals(Branch.of(branchName, someHash), createdBranch1);
    Reference createdBranch2 =
        api.createReference()
            .sourceRefName(main.getName())
            .reference(Branch.of(branchName2, someHash))
            .submit();
    assertEquals(Branch.of(branchName2, someHash), createdBranch2);

    Map<String, Reference> references =
        api.getAllReferences().submit().stream()
            .filter(r -> root.equals(r.getName()) || r.getName().endsWith(refNamePart))
            .collect(Collectors.toMap(Reference::getName, Function.identity()));

    assertThat(references)
        .containsAllEntriesOf(
            ImmutableMap.of(
                main.getName(),
                main,
                createdTag.getName(),
                createdTag,
                createdBranch1.getName(),
                createdBranch1,
                createdBranch2.getName(),
                createdBranch2));
    assertThat(references.get(main.getName())).isInstanceOf(Branch.class);
    assertThat(references.get(createdTag.getName())).isInstanceOf(Tag.class);
    assertThat(references.get(createdBranch1.getName())).isInstanceOf(Branch.class);
    assertThat(references.get(createdBranch2.getName())).isInstanceOf(Branch.class);

    Reference tagRef = references.get(tagName);
    Reference branchRef = references.get(branchName);
    Reference branchRef2 = references.get(branchName2);

    String tagHash = tagRef.getHash();
    String branchHash = branchRef.getHash();
    String branchHash2 = branchRef2.getHash();

    assertThat(api.getReference().refName(tagName).submit()).isEqualTo(tagRef);
    assertThat(api.getReference().refName(branchName).submit()).isEqualTo(branchRef);

    EntriesResponse entries = api.getEntries().refName(tagName).submit();
    assertThat(entries).isNotNull();
    entries = api.getEntries().refName(branchName).submit();
    assertThat(entries).isNotNull();

    LogResponse log = api.getCommitLog().refName(tagName).submit();
    assertThat(log).isNotNull();
    log = api.getCommitLog().refName(branchName).submit();
    assertThat(log).isNotNull();

    // Need to have at least one op, otherwise all following operations (assignTag/Branch, merge,
    // delete) will fail
    meta = IcebergTable.of("foo", -1L);
    api.commitMultipleOperations()
        .branchName(branchName)
        .hash(branchHash)
        .operation(Put.of(ContentsKey.of("some-key"), meta))
        .commitMeta(CommitMeta.fromMessage("One dummy op"))
        .submit();
    log = api.getCommitLog().refName(branchName).submit();
    String newHash = log.getOperations().get(0).getHash();

    api.assignTag()
        .tagName(tagName)
        .hash(tagHash)
        .assignTo(Branch.of(branchName, newHash))
        .submit();
    api.assignBranch()
        .branchName(branchName)
        .hash(newHash)
        .assignTo(Branch.of(branchName, newHash))
        .submit();

    api.mergeRefIntoBranch()
        .branchName(branchName2)
        .hash(branchHash2)
        .fromRefName(branchName)
        .fromHash(newHash)
        .submit();

    api.deleteTag().tagName(tagName).hash(newHash).submit();
    api.deleteBranch().branchName(branchName).hash(newHash).submit();
  }

  @Test
  public void filterCommitLogByAuthor() throws NessieNotFoundException, NessieConflictException {
    Reference main = api.getReference().refName("main").submit();
    Branch filterCommitLogByAuthor = Branch.of("filterCommitLogByAuthor", main.getHash());
    Reference branch =
        api.createReference()
            .sourceRefName(main.getName())
            .reference(filterCommitLogByAuthor)
            .submit();
    assertThat(branch).isEqualTo(filterCommitLogByAuthor);

    int numAuthors = 5;
    int commitsPerAuthor = 10;

    String currentHash = main.getHash();
    createCommits(branch, numAuthors, commitsPerAuthor, currentHash);
    LogResponse log = api.getCommitLog().refName(branch.getName()).submit();
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(numAuthors * commitsPerAuthor);

    log =
        api.getCommitLog()
            .refName(branch.getName())
            .queryExpression("commit.author == 'author-3'")
            .submit();
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(commitsPerAuthor);
    log.getOperations().forEach(commit -> assertThat(commit.getAuthor()).isEqualTo("author-3"));

    log =
        api.getCommitLog()
            .refName(branch.getName())
            .queryExpression(
                "commit.author == 'author-3' && commit.committer == 'random-committer'")
            .submit();
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).isEmpty();

    log =
        api.getCommitLog()
            .refName(branch.getName())
            .queryExpression("commit.author == 'author-3' && commit.committer == ''")
            .submit();
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(commitsPerAuthor);
    log.getOperations().forEach(commit -> assertThat(commit.getAuthor()).isEqualTo("author-3"));

    log =
        api.getCommitLog()
            .refName(branch.getName())
            .queryExpression("commit.author in ['author-1', 'author-3', 'author-4']")
            .submit();
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(commitsPerAuthor * 3);
    log.getOperations()
        .forEach(
            commit ->
                assertThat(ImmutableList.of("author-1", "author-3", "author-4"))
                    .contains(commit.getAuthor()));

    log =
        api.getCommitLog()
            .refName(branch.getName())
            .queryExpression("!(commit.author in ['author-1', 'author-0'])")
            .submit();
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(commitsPerAuthor * 3);
    log.getOperations()
        .forEach(
            commit ->
                assertThat(ImmutableList.of("author-2", "author-3", "author-4"))
                    .contains(commit.getAuthor()));

    log =
        api.getCommitLog()
            .refName(branch.getName())
            .queryExpression("commit.author.matches('au.*-(2|4)')")
            .submit();
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(commitsPerAuthor * 2);
    log.getOperations()
        .forEach(
            commit ->
                assertThat(ImmutableList.of("author-2", "author-4")).contains(commit.getAuthor()));
  }

  @Test
  public void filterCommitLogByTimeRange() throws NessieNotFoundException, NessieConflictException {
    Reference main = api.getReference().refName("main").submit();
    Branch filterCommitLogByAuthor = Branch.of("filterCommitLogByTimeRange", main.getHash());
    Reference branch =
        api.createReference()
            .sourceRefName(main.getName())
            .reference(filterCommitLogByAuthor)
            .submit();
    assertThat(branch).isEqualTo(filterCommitLogByAuthor);

    int numAuthors = 5;
    int commitsPerAuthor = 10;
    int expectedTotalSize = numAuthors * commitsPerAuthor;

    String currentHash = main.getHash();
    createCommits(branch, numAuthors, commitsPerAuthor, currentHash);
    LogResponse log = api.getCommitLog().refName(branch.getName()).submit();
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(expectedTotalSize);

    Instant initialCommitTime =
        log.getOperations().get(log.getOperations().size() - 1).getCommitTime();
    assertThat(initialCommitTime).isNotNull();
    Instant lastCommitTime = log.getOperations().get(0).getCommitTime();
    assertThat(lastCommitTime).isNotNull();
    Instant fiveMinLater = initialCommitTime.plus(5, ChronoUnit.MINUTES);

    log =
        api.getCommitLog()
            .refName(branch.getName())
            .queryExpression(
                String.format("timestamp(commit.commitTime) > timestamp('%s')", initialCommitTime))
            .submit();
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(expectedTotalSize - 1);
    log.getOperations()
        .forEach(commit -> assertThat(commit.getCommitTime()).isAfter(initialCommitTime));

    log =
        api.getCommitLog()
            .refName(branch.getName())
            .queryExpression(
                String.format("timestamp(commit.commitTime) < timestamp('%s')", fiveMinLater))
            .submit();
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(expectedTotalSize);
    log.getOperations()
        .forEach(commit -> assertThat(commit.getCommitTime()).isBefore(fiveMinLater));

    log =
        api.getCommitLog()
            .refName(branch.getName())
            .queryExpression(
                String.format(
                    "timestamp(commit.commitTime) > timestamp('%s') && timestamp(commit.commitTime) < timestamp('%s')",
                    initialCommitTime, lastCommitTime))
            .submit();
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(expectedTotalSize - 2);
    log.getOperations()
        .forEach(
            commit ->
                assertThat(commit.getCommitTime())
                    .isAfter(initialCommitTime)
                    .isBefore(lastCommitTime));

    log =
        api.getCommitLog()
            .refName(branch.getName())
            .queryExpression(
                String.format("timestamp(commit.commitTime) > timestamp('%s')", fiveMinLater))
            .submit();
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).isEmpty();
  }

  @Test
  public void filterCommitLogByProperties()
      throws NessieNotFoundException, NessieConflictException {
    Reference main = api.getReference().refName("main").submit();
    Branch filterCommitLogByAuthor = Branch.of("filterCommitLogByProperties", main.getHash());
    Reference branch =
        api.createReference()
            .sourceRefName(main.getName())
            .reference(filterCommitLogByAuthor)
            .submit();
    assertThat(branch).isEqualTo(filterCommitLogByAuthor);

    int numAuthors = 5;
    int commitsPerAuthor = 10;

    String currentHash = main.getHash();
    createCommits(branch, numAuthors, commitsPerAuthor, currentHash);
    LogResponse log = api.getCommitLog().refName(branch.getName()).submit();
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(numAuthors * commitsPerAuthor);

    log =
        api.getCommitLog()
            .refName(branch.getName())
            .queryExpression("commit.properties['prop1'] == 'val1'")
            .submit();
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(numAuthors * commitsPerAuthor);
    log.getOperations()
        .forEach(commit -> assertThat(commit.getProperties().get("prop1")).isEqualTo("val1"));

    log =
        api.getCommitLog()
            .refName(branch.getName())
            .queryExpression("commit.properties['prop1'] == 'val3'")
            .submit();
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).isEmpty();
  }

  @Test
  public void filterCommitLogByCommitRange()
      throws NessieNotFoundException, NessieConflictException {
    Reference main = api.getReference().refName("main").submit();
    Branch b = Branch.of("filterCommitLogByCommitRange", main.getHash());
    Reference branch = api.createReference().sourceRefName(main.getName()).reference(b).submit();
    assertThat(branch).isEqualTo(b);

    int numCommits = 10;

    String currentHash = main.getHash();
    createCommits(branch, 1, numCommits, currentHash);
    LogResponse entireLog = api.getCommitLog().refName(branch.getName()).submit();
    assertThat(entireLog).isNotNull();
    assertThat(entireLog.getOperations()).hasSize(numCommits);

    // if startHash > endHash, then we return all commits starting from startHash
    String startHash = entireLog.getOperations().get(numCommits / 2).getHash();
    String endHash = entireLog.getOperations().get(0).getHash();
    LogResponse log =
        api.getCommitLog()
            .refName(branch.getName())
            .hashOnRef(endHash)
            .untilHash(startHash)
            .submit();
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(numCommits / 2 + 1);

    for (int i = 0, j = numCommits - 1; i < j; i++, j--) {
      startHash = entireLog.getOperations().get(j).getHash();
      endHash = entireLog.getOperations().get(i).getHash();
      log =
          api.getCommitLog()
              .refName(branch.getName())
              .hashOnRef(endHash)
              .untilHash(startHash)
              .submit();
      assertThat(log).isNotNull();
      assertThat(log.getOperations()).hasSize(numCommits - (i * 2));
      assertThat(ImmutableList.copyOf(entireLog.getOperations()).subList(i, j + 1))
          .containsExactlyElementsOf(log.getOperations());
    }
  }

  private void createCommits(
      Reference branch, int numAuthors, int commitsPerAuthor, String currentHash)
      throws NessieNotFoundException, NessieConflictException {
    for (int j = 0; j < numAuthors; j++) {
      String author = "author-" + j;
      for (int i = 0; i < commitsPerAuthor; i++) {
        IcebergTable meta = IcebergTable.of("some-file-" + i, -1L);
        String nextHash =
            api.commitMultipleOperations()
                .branchName(branch.getName())
                .hash(currentHash)
                .commitMeta(
                    CommitMeta.builder()
                        .author(author)
                        .message("committed-by-" + author)
                        .properties(ImmutableMap.of("prop1", "val1", "prop2", "val2"))
                        .build())
                .operation(Put.of(ContentsKey.of("table" + i), meta))
                .submit()
                .getHash();
        assertThat(currentHash).isNotEqualTo(nextHash);
        currentHash = nextHash;
      }
    }
  }

  @Test
  void commitLogPagingAndFilteringByAuthor()
      throws NessieNotFoundException, NessieConflictException {
    String someHash = api.getReference().refName("main").submit().getHash();
    String branchName = "commitLogPagingAndFiltering";
    Branch branch = Branch.of(branchName, someHash);
    api.createReference().sourceRefName("main").reference(branch).submit();

    int numAuthors = 3;
    int commits = 45;
    int pageSizeHint = 10;
    int expectedTotalSize = numAuthors * commits;

    createCommits(branch, numAuthors, commits, someHash);
    LogResponse log = api.getCommitLog().refName(branch.getName()).submit();
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(expectedTotalSize);

    String author = "author-1";
    List<String> messagesOfAuthorOne =
        log.getOperations().stream()
            .filter(c -> author.equals(c.getAuthor()))
            .map(CommitMeta::getMessage)
            .collect(Collectors.toList());
    verifyPaging(branchName, commits, pageSizeHint, messagesOfAuthorOne, author);

    List<String> allMessages =
        log.getOperations().stream().map(CommitMeta::getMessage).collect(Collectors.toList());
    List<CommitMeta> completeLog =
        StreamingUtil.getCommitLogStream(
                api, branchName, null, null, null, OptionalInt.of(pageSizeHint))
            .collect(Collectors.toList());
    assertThat(completeLog.stream().map(CommitMeta::getMessage))
        .containsExactlyElementsOf(allMessages);
  }

  @Test
  void commitLogPaging() throws NessieNotFoundException, NessieConflictException {
    String someHash = api.getReference().refName("main").submit().getHash();
    String branchName = "commitLogPaging";
    Branch branch = Branch.of(branchName, someHash);
    api.createReference().sourceRefName("main").reference(branch).submit();

    int commits = 95;
    int pageSizeHint = 10;

    String currentHash = someHash;
    List<String> allMessages = new ArrayList<>();
    for (int i = 0; i < commits; i++) {
      String msg = "message-for-" + i;
      allMessages.add(msg);
      IcebergTable tableMeta = IcebergTable.of("some-file-" + i, -1L);
      String nextHash =
          api.commitMultipleOperations()
              .branchName(branchName)
              .hash(currentHash)
              .commitMeta(CommitMeta.fromMessage(msg))
              .operation(Put.of(ContentsKey.of("table"), tableMeta))
              .submit()
              .getHash();
      assertNotEquals(currentHash, nextHash);
      currentHash = nextHash;
    }
    Collections.reverse(allMessages);

    verifyPaging(branchName, commits, pageSizeHint, allMessages, null);

    List<CommitMeta> completeLog =
        StreamingUtil.getCommitLogStream(
                api, branchName, null, null, null, OptionalInt.of(pageSizeHint))
            .collect(Collectors.toList());
    assertEquals(
        completeLog.stream().map(CommitMeta::getMessage).collect(Collectors.toList()), allMessages);
  }

  private void verifyPaging(
      String branchName,
      int commits,
      int pageSizeHint,
      List<String> commitMessages,
      String filterByAuthor)
      throws NessieNotFoundException {
    String pageToken = null;
    for (int pos = 0; pos < commits; pos += pageSizeHint) {
      String queryExpression = null;
      if (null != filterByAuthor) {
        queryExpression = String.format("commit.author=='%s'", filterByAuthor);
      }
      LogResponse response =
          api.getCommitLog()
              .refName(branchName)
              .maxRecords(pageSizeHint)
              .pageToken(pageToken)
              .queryExpression(queryExpression)
              .submit();
      if (pos + pageSizeHint <= commits) {
        assertTrue(response.hasMore());
        assertNotNull(response.getToken());
        assertEquals(
            commitMessages.subList(pos, pos + pageSizeHint),
            response.getOperations().stream()
                .map(CommitMeta::getMessage)
                .collect(Collectors.toList()));
        pageToken = response.getToken();
      } else {
        assertFalse(response.hasMore());
        assertNull(response.getToken());
        assertEquals(
            commitMessages.subList(pos, commitMessages.size()),
            response.getOperations().stream()
                .map(CommitMeta::getMessage)
                .collect(Collectors.toList()));
        break;
      }
    }
  }

  @Test
  void multiget() throws NessieNotFoundException, NessieConflictException {
    final String branch = "foo";
    Reference r =
        api.createReference().sourceRefName("main").reference(Branch.of(branch, null)).submit();
    ContentsKey a = ContentsKey.of("a");
    ContentsKey b = ContentsKey.of("b");
    IcebergTable ta = IcebergTable.of("path1", -1L);
    IcebergTable tb = IcebergTable.of("path2", -1L);
    api.commitMultipleOperations()
        .branchName(branch)
        .hash(r.getHash())
        .operation(Put.of(a, ta))
        .commitMeta(CommitMeta.fromMessage("commit 1"))
        .submit();
    api.commitMultipleOperations()
        .branchName(branch)
        .hash(r.getHash())
        .operation(Put.of(b, tb))
        .commitMeta(CommitMeta.fromMessage("commit 2"))
        .submit();
    Map<ContentsKey, Contents> response =
        api.getContents().key(a).key(b).key(ContentsKey.of("noexist")).refName("foo").submit();
    assertThat(response)
        .containsEntry(a, ta)
        .containsEntry(b, tb)
        .doesNotContainKey(ContentsKey.of("noexist"));
    api.deleteBranch()
        .branchName(branch)
        .hash(api.getReference().refName(branch).submit().getHash())
        .submit();
  }

  private static final class ContentAndOperationType {
    final Type type;
    final Operation operation;
    final Operation globalOperation;

    ContentAndOperationType(Type type, Operation operation) {
      this(type, operation, null);
    }

    ContentAndOperationType(Type type, Operation operation, Operation globalOperation) {
      this.type = type;
      this.operation = operation;
      this.globalOperation = globalOperation;
    }

    @Override
    public String toString() {
      String s = opString(operation);
      if (globalOperation != null) {
        s = "_" + opString(globalOperation);
      }
      return s + "_" + operation.getKey().toPathString();
    }

    private static String opString(Operation operation) {
      if (operation instanceof Put) {
        return "Put_" + ((Put) operation).getContents().getClass().getSimpleName();
      } else {
        return operation.getClass().getSimpleName();
      }
    }
  }

  static Stream<ContentAndOperationType> contentAndOperationTypes() {
    return Stream.of(
        new ContentAndOperationType(
            Type.ICEBERG_TABLE,
            Put.of(ContentsKey.of("iceberg"), IcebergTable.of("/iceberg/table", -1L))),
        new ContentAndOperationType(
            Type.VIEW,
            Put.of(
                ContentsKey.of("view_dremio"),
                ImmutableSqlView.builder()
                    .dialect(Dialect.DREMIO)
                    .sqlText("SELECT foo FROM dremio")
                    .build())),
        new ContentAndOperationType(
            Type.VIEW,
            Put.of(
                ContentsKey.of("view_presto"),
                ImmutableSqlView.builder()
                    .dialect(Dialect.PRESTO)
                    .sqlText("SELECT foo FROM presto")
                    .build())),
        new ContentAndOperationType(
            Type.VIEW,
            Put.of(
                ContentsKey.of("view_spark"),
                ImmutableSqlView.builder()
                    .dialect(Dialect.SPARK)
                    .sqlText("SELECT foo FROM spark")
                    .build())),
        new ContentAndOperationType(
            Type.DELTA_LAKE_TABLE,
            Put.of(
                ContentsKey.of("delta"),
                ImmutableDeltaLakeTable.builder()
                    .addCheckpointLocationHistory("checkpoint")
                    .addMetadataLocationHistory("metadata")
                    .build())),
        new ContentAndOperationType(
            Type.ICEBERG_TABLE, Delete.of(ContentsKey.of("iceberg_delete"))),
        new ContentAndOperationType(
            Type.ICEBERG_TABLE, Unchanged.of(ContentsKey.of("iceberg_unchanged"))),
        new ContentAndOperationType(Type.VIEW, Delete.of(ContentsKey.of("view_dremio_delete"))),
        new ContentAndOperationType(
            Type.VIEW, Unchanged.of(ContentsKey.of("view_dremio_unchanged"))),
        new ContentAndOperationType(Type.VIEW, Delete.of(ContentsKey.of("view_spark_delete"))),
        new ContentAndOperationType(
            Type.VIEW, Unchanged.of(ContentsKey.of("view_spark_unchanged"))),
        new ContentAndOperationType(
            Type.DELTA_LAKE_TABLE, Delete.of(ContentsKey.of("delta_delete"))),
        new ContentAndOperationType(
            Type.DELTA_LAKE_TABLE, Unchanged.of(ContentsKey.of("delta_unchanged"))));
  }

  @Test
  void verifyAllContentAndOperationTypes() throws NessieNotFoundException, NessieConflictException {
    String branchName = "contentAndOperationAll";
    Reference r =
        api.createReference().sourceRefName("main").reference(Branch.of(branchName, null)).submit();

    CommitMultipleOperationsBuilder commit =
        api.commitMultipleOperations()
            .branchName(branchName)
            .hash(r.getHash())
            .commitMeta(CommitMeta.fromMessage("verifyAllContentAndOperationTypes"));
    contentAndOperationTypes()
        .flatMap(
            c ->
                c.globalOperation == null
                    ? Stream.of(c.operation)
                    : Stream.of(c.operation, c.globalOperation))
        .forEach(commit::operation);
    commit.submit();

    List<Entry> entries = api.getEntries().refName(branchName).submit().getEntries();
    List<Entry> expect =
        contentAndOperationTypes()
            .filter(c -> c.operation instanceof Put)
            .map(c -> Entry.builder().type(c.type).name(c.operation.getKey()).build())
            .collect(Collectors.toList());
    assertThat(entries).containsExactlyInAnyOrderElementsOf(expect);
  }

  @ParameterizedTest
  @MethodSource("contentAndOperationTypes")
  void verifyContentAndOperationTypesIndividually(ContentAndOperationType contentAndOperationType)
      throws NessieNotFoundException, NessieConflictException {
    String branchName = "contentAndOperation_" + contentAndOperationType;
    Reference r =
        api.createReference().sourceRefName("main").reference(Branch.of(branchName, null)).submit();
    CommitMultipleOperationsBuilder commit =
        api.commitMultipleOperations()
            .branchName(branchName)
            .hash(r.getHash())
            .commitMeta(CommitMeta.fromMessage("commit " + contentAndOperationType))
            .operation(contentAndOperationType.operation);
    if (contentAndOperationType.globalOperation != null) {
      commit.operation(contentAndOperationType.globalOperation);
    }
    commit.submit();
    List<Entry> entries = api.getEntries().refName(branchName).submit().getEntries();
    // Oh, yea - this is weird. The property ContentAndOperationType.operation.key.namespace is null
    // (!!!)
    // here, because somehow JUnit @MethodSource implementation re-constructs the objects returned
    // from
    // the source-method contentAndOperationTypes.
    ContentsKey fixedContentKey =
        ContentsKey.of(contentAndOperationType.operation.getKey().getElements());
    List<Entry> expect =
        contentAndOperationType.operation instanceof Put
            ? singletonList(
                Entry.builder().name(fixedContentKey).type(contentAndOperationType.type).build())
            : emptyList();
    assertThat(entries).containsExactlyInAnyOrderElementsOf(expect);
  }

  @Test
  void filterEntriesByType() throws NessieNotFoundException, NessieConflictException {
    final String branch = "filterTypes";
    Reference r =
        api.createReference().sourceRefName("main").reference(Branch.of(branch, null)).submit();
    ContentsKey a = ContentsKey.of("a");
    ContentsKey b = ContentsKey.of("b");
    IcebergTable tam = IcebergTable.of("path1", -1L);
    SqlView tb =
        ImmutableSqlView.builder().sqlText("select * from table").dialect(Dialect.DREMIO).build();
    api.commitMultipleOperations()
        .branchName(branch)
        .hash(r.getHash())
        .operation(Put.of(a, tam))
        .commitMeta(CommitMeta.fromMessage("commit 1"))
        .submit();
    api.commitMultipleOperations()
        .branchName(branch)
        .hash(r.getHash())
        .operation(Put.of(b, tb))
        .commitMeta(CommitMeta.fromMessage("commit 2"))
        .submit();
    List<Entry> entries = api.getEntries().refName(branch).submit().getEntries();
    List<Entry> expected =
        asList(
            Entry.builder().name(a).type(Type.ICEBERG_TABLE).build(),
            Entry.builder().name(b).type(Type.VIEW).build());
    assertThat(entries).containsExactlyInAnyOrderElementsOf(expected);

    entries =
        api.getEntries()
            .refName(branch)
            .queryExpression("entry.contentType=='ICEBERG_TABLE'")
            .submit()
            .getEntries();
    assertEquals(singletonList(expected.get(0)), entries);

    entries =
        api.getEntries()
            .refName(branch)
            .queryExpression("entry.contentType=='VIEW'")
            .submit()
            .getEntries();
    assertEquals(singletonList(expected.get(1)), entries);

    entries =
        api.getEntries()
            .refName(branch)
            .queryExpression("entry.contentType in ['ICEBERG_TABLE', 'VIEW']")
            .submit()
            .getEntries();
    assertThat(entries).containsExactlyInAnyOrderElementsOf(expected);

    api.deleteBranch()
        .branchName(branch)
        .hash(api.getReference().refName(branch).submit().getHash())
        .submit();
  }

  @Test
  public void filterEntriesByNamespace() throws NessieConflictException, NessieNotFoundException {
    final String branch = "filterEntriesByNamespace";
    Reference r =
        api.createReference().sourceRefName("main").reference(Branch.of(branch, null)).submit();
    ContentsKey first = ContentsKey.of("a", "b", "c", "firstTable");
    ContentsKey second = ContentsKey.of("a", "b", "c", "secondTable");
    ContentsKey third = ContentsKey.of("a", "thirdTable");
    ContentsKey fourth = ContentsKey.of("a", "fourthTable");
    api.commitMultipleOperations()
        .branchName(branch)
        .hash(r.getHash())
        .operation(Put.of(first, IcebergTable.of("path1", -1L)))
        .commitMeta(CommitMeta.fromMessage("commit 1"))
        .submit();
    api.commitMultipleOperations()
        .branchName(branch)
        .hash(r.getHash())
        .operation(Put.of(second, IcebergTable.of("path2", -1L)))
        .commitMeta(CommitMeta.fromMessage("commit 2"))
        .submit();
    api.commitMultipleOperations()
        .branchName(branch)
        .hash(r.getHash())
        .operation(Put.of(third, IcebergTable.of("path3", -1L)))
        .commitMeta(CommitMeta.fromMessage("commit 3"))
        .submit();
    api.commitMultipleOperations()
        .branchName(branch)
        .hash(r.getHash())
        .operation(Put.of(fourth, IcebergTable.of("path4", -1L)))
        .commitMeta(CommitMeta.fromMessage("commit 4"))
        .submit();

    List<Entry> entries = api.getEntries().refName(branch).submit().getEntries();
    assertThat(entries).isNotNull().hasSize(4);

    entries = api.getEntries().refName(branch).submit().getEntries();
    assertThat(entries).isNotNull().hasSize(4);

    entries =
        api.getEntries()
            .refName(branch)
            .queryExpression("entry.namespace.startsWith('a.b')")
            .submit()
            .getEntries();
    assertThat(entries).hasSize(2);
    entries.forEach(e -> assertThat(e.getName().getNamespace().name()).startsWith("a.b"));

    entries =
        api.getEntries()
            .refName(branch)
            .queryExpression("entry.namespace.startsWith('a')")
            .submit()
            .getEntries();
    assertThat(entries).hasSize(4);
    entries.forEach(e -> assertThat(e.getName().getNamespace().name()).startsWith("a"));

    entries =
        api.getEntries()
            .refName(branch)
            .queryExpression("entry.namespace.startsWith('a.b.c.firstTable')")
            .submit()
            .getEntries();
    assertThat(entries).isEmpty();

    entries =
        api.getEntries()
            .refName(branch)
            .queryExpression("entry.namespace.startsWith('a.fourthTable')")
            .submit()
            .getEntries();
    assertThat(entries).isEmpty();

    api.deleteBranch()
        .branchName(branch)
        .hash(api.getReference().refName(branch).submit().getHash())
        .submit();
  }

  @Test
  public void filterEntriesByNamespaceAndPrefixDepth()
      throws NessieConflictException, NessieNotFoundException {
    final String branch = "filterEntriesByNamespaceAndPrefixDepth";
    Reference r =
        api.createReference().sourceRefName("main").reference(Branch.of(branch, null)).submit();
    ContentsKey first = ContentsKey.of("a", "b", "c", "firstTable");
    ContentsKey second = ContentsKey.of("a", "b", "c", "secondTable");
    ContentsKey third = ContentsKey.of("a", "thirdTable");
    ContentsKey fourth = ContentsKey.of("a", "b", "fourthTable");
    ContentsKey fifth = ContentsKey.of("a", "boo", "fifthTable");
    List<ContentsKey> keys = ImmutableList.of(first, second, third, fourth, fifth);
    for (int i = 0; i < 5; i++) {
      api.commitMultipleOperations()
          .branchName(branch)
          .hash(r.getHash())
          .operation(Put.of(keys.get(i), IcebergTable.of("path" + i, -1L)))
          .commitMeta(CommitMeta.fromMessage("commit " + i))
          .submit();
    }

    List<Entry> entries = api.getEntries().refName(branch).namespaceDepth(0).submit().getEntries();
    assertThat(entries).isNotNull().hasSize(5);

    entries =
        api.getEntries()
            .refName(branch)
            .namespaceDepth(0)
            .queryExpression("entry.namespace.matches('a(\\\\.|$)')")
            .submit()
            .getEntries();
    assertThat(entries).isNotNull().hasSize(5);

    entries =
        api.getEntries()
            .refName(branch)
            .namespaceDepth(1)
            .queryExpression("entry.namespace.matches('a(\\\\.|$)')")
            .submit()
            .getEntries();
    assertThat(entries).hasSize(1);
    assertThat(entries.stream().map(e -> e.getName().toPathString()))
        .containsExactlyInAnyOrder("a");

    entries =
        api.getEntries()
            .refName(branch)
            .namespaceDepth(2)
            .queryExpression("entry.namespace.matches('a(\\\\.|$)')")
            .submit()
            .getEntries();
    assertThat(entries).hasSize(3);
    assertThat(entries.stream().map(e -> e.getName().toPathString()))
        .containsExactlyInAnyOrder("a.thirdTable", "a.b", "a.boo");

    entries =
        api.getEntries()
            .refName(branch)
            .namespaceDepth(3)
            .queryExpression("entry.namespace.matches('a\\\\.b(\\\\.|$)')")
            .submit()
            .getEntries();
    assertThat(entries).hasSize(2);
    assertThat(entries.stream().map(e -> e.getName().toPathString()))
        .containsExactlyInAnyOrder("a.b.c", "a.b.fourthTable");

    entries =
        api.getEntries()
            .refName(branch)
            .namespaceDepth(4)
            .queryExpression("entry.namespace.matches('a\\\\.b\\\\.c(\\\\.|$)')")
            .submit()
            .getEntries();
    assertThat(entries).hasSize(2);
    assertThat(entries.stream().map(e -> e.getName().toPathString()))
        .containsExactlyInAnyOrder("a.b.c.firstTable", "a.b.c.secondTable");

    api.deleteBranch()
        .branchName(branch)
        .hash(api.getReference().refName(branch).submit().getHash())
        .submit();
  }

  @Test
  public void checkCelScriptFailureReporting() {
    assertThatThrownBy(
            () -> api.getEntries().refName("main").queryExpression("invalid_script").submit())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageContaining("undeclared reference to 'invalid_script'");

    assertThatThrownBy(
            () -> api.getCommitLog().refName("main").queryExpression("invalid_script").submit())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageContaining("undeclared reference to 'invalid_script'");
  }

  @Test
  void checkSpecialCharacterRoundTrip() throws NessieNotFoundException, NessieConflictException {
    final String branch = "specialchar";
    Reference r =
        api.createReference().sourceRefName("main").reference(Branch.of(branch, null)).submit();
    // ContentsKey k = ContentsKey.of("/%国","国.国");
    ContentsKey k = ContentsKey.of("a.b", "c.txt");
    IcebergTable ta = IcebergTable.of("path1", -1L);
    api.commitMultipleOperations()
        .branchName(branch)
        .hash(r.getHash())
        .operation(Put.of(k, ta))
        .commitMeta(CommitMeta.fromMessage("commit 1"))
        .submit();

    assertThat(api.getContents().key(k).refName(branch).submit()).containsEntry(k, ta);
    assertEquals(ta, api.getContents().key(k).refName(branch).submit().get(k));
    api.deleteBranch()
        .branchName(branch)
        .hash(api.getReference().refName(branch).submit().getHash())
        .submit();
  }

  @Test
  void checkServerErrorPropagation() throws NessieNotFoundException, NessieConflictException {
    final String branch = "bar";
    api.createReference().sourceRefName("main").reference(Branch.of(branch, null)).submit();
    assertThatThrownBy(
            () ->
                api.createReference()
                    .sourceRefName("main")
                    .reference(Branch.of(branch, null))
                    .submit())
        .isInstanceOf(NessieConflictException.class)
        .hasMessageContaining("already exists");
  }

  @ParameterizedTest
  @CsvSource({
    "x/" + COMMA_VALID_HASH_1,
    "abc'" + COMMA_VALID_HASH_1,
    ".foo" + COMMA_VALID_HASH_2,
    "abc'def'..'blah" + COMMA_VALID_HASH_2,
    "abc'de..blah" + COMMA_VALID_HASH_3,
    "abc'de@{blah" + COMMA_VALID_HASH_3
  })
  void invalidBranchNames(String invalidBranchName, String validHash) {
    ContentsKey key = ContentsKey.of("x");
    Tag tag = Tag.of("valid", validHash);

    String opsCountMsg = ".operations.operations: size must be between 1 and 2147483647";

    assertAll(
        () ->
            assertThatThrownBy(
                    () ->
                        api.commitMultipleOperations()
                            .branchName(invalidBranchName)
                            .hash(validHash)
                            .commitMeta(CommitMeta.fromMessage(""))
                            .submit())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(".branchName: " + REF_NAME_MESSAGE)
                .hasMessageContaining(opsCountMsg),
        () ->
            assertThatThrownBy(
                    () -> api.deleteBranch().branchName(invalidBranchName).hash(validHash).submit())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("deleteBranch.branchName: " + REF_NAME_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        api.getCommitLog().refName(invalidBranchName).untilHash(validHash).submit())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("getCommitLog.ref: " + REF_NAME_MESSAGE),
        () ->
            assertThatThrownBy(
                    () -> api.getEntries().refName(invalidBranchName).hashOnRef(validHash).submit())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("getEntries.refName: " + REF_NAME_MESSAGE),
        () ->
            assertThatThrownBy(() -> api.getReference().refName(invalidBranchName).submit())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("getReferenceByName.refName: " + REF_NAME_OR_HASH_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        api.assignTag()
                            .tagName(invalidBranchName)
                            .hash(validHash)
                            .assignTo(tag)
                            .submit())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("assignTag.tagName: " + REF_NAME_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        httpClient
                            .newRequest()
                            .path("trees/branch/{branchName}/merge")
                            .resolveTemplate("branchName", invalidBranchName)
                            .queryParam("expectedHash", validHash)
                            .post(null))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("mergeRefIntoBranch.branchName: " + REF_NAME_MESSAGE)
                .hasMessageContaining("mergeRefIntoBranch.merge: must not be null"),
        () ->
            assertThatThrownBy(
                    () ->
                        api.mergeRefIntoBranch()
                            .branchName(invalidBranchName)
                            .hash(validHash)
                            .fromRef(api.getDefaultBranch())
                            .submit())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("mergeRefIntoBranch.branchName: " + REF_NAME_MESSAGE),
        () ->
            assertThatThrownBy(
                    () -> api.deleteTag().tagName(invalidBranchName).hash(validHash).submit())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("deleteTag.tagName: " + REF_NAME_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        api.transplantCommitsIntoBranch()
                            .branchName(invalidBranchName)
                            .hash(validHash)
                            .fromRefName("main")
                            .hashesToTransplant(
                                singletonList(
                                    api.getReference().refName("main").submit().getHash()))
                            .submit())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(
                    "transplantCommitsIntoBranch.branchName: " + REF_NAME_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        api.getContents()
                            .key(key)
                            .refName(invalidBranchName)
                            .hashOnRef(validHash)
                            .submit())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(".ref: " + REF_NAME_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        api.getContents()
                            .key(key)
                            .refName(invalidBranchName)
                            .hashOnRef(validHash)
                            .submit())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(".ref: " + REF_NAME_MESSAGE));
  }

  @ParameterizedTest
  @CsvSource({
    "abc'" + COMMA_VALID_HASH_1,
    ".foo" + COMMA_VALID_HASH_2,
    "abc'def'..'blah" + COMMA_VALID_HASH_2,
    "abc'de..blah" + COMMA_VALID_HASH_3,
    "abc'de@{blah" + COMMA_VALID_HASH_3
  })
  void invalidHashes(String invalidHashIn, String validHash) {
    // CsvSource maps an empty string as null
    String invalidHash = invalidHashIn != null ? invalidHashIn : "";

    String validBranchName = "hello";
    ContentsKey key = ContentsKey.of("x");
    Tag tag = Tag.of("valid", validHash);

    String opsCountMsg = ".operations.operations: size must be between 1 and 2147483647";

    assertAll(
        () ->
            assertThatThrownBy(
                    () ->
                        api.commitMultipleOperations()
                            .branchName(validBranchName)
                            .hash(invalidHash)
                            .commitMeta(CommitMeta.fromMessage(""))
                            .submit())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(".hash: " + HASH_MESSAGE)
                .hasMessageContaining(opsCountMsg),
        () ->
            assertThatThrownBy(
                    () -> api.deleteBranch().branchName(validBranchName).hash(invalidHash).submit())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("deleteBranch.hash: " + HASH_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        api.assignTag()
                            .tagName(validBranchName)
                            .hash(invalidHash)
                            .assignTo(tag)
                            .submit())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("assignTag.oldHash: " + HASH_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        httpClient
                            .newRequest()
                            .path("trees/branch/{branchName}/merge")
                            .resolveTemplate("branchName", validBranchName)
                            .queryParam("expectedHash", invalidHash)
                            .post(null))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("mergeRefIntoBranch.merge: must not be null")
                .hasMessageContaining("mergeRefIntoBranch.hash: " + HASH_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        api.mergeRefIntoBranch()
                            .branchName(validBranchName)
                            .hash(invalidHash)
                            .fromRef(api.getDefaultBranch())
                            .submit())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("mergeRefIntoBranch.hash: " + HASH_MESSAGE),
        () ->
            assertThatThrownBy(
                    () -> api.deleteTag().tagName(validBranchName).hash(invalidHash).submit())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("deleteTag.hash: " + HASH_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        api.transplantCommitsIntoBranch()
                            .branchName(validBranchName)
                            .hash(invalidHash)
                            .fromRefName("main")
                            .hashesToTransplant(
                                singletonList(
                                    api.getReference().refName("main").submit().getHash()))
                            .submit())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("transplantCommitsIntoBranch.hash: " + HASH_MESSAGE),
        () ->
            assertThatThrownBy(() -> api.getContents().refName(invalidHash).submit())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(
                    ".request.requestedKeys: size must be between 1 and 2147483647")
                .hasMessageContaining(".ref: " + REF_NAME_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        api.getContents().refName(validBranchName).hashOnRef(invalidHash).submit())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(
                    ".request.requestedKeys: size must be between 1 and 2147483647")
                .hasMessageContaining(".hashOnRef: " + HASH_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        api.getContents()
                            .key(key)
                            .refName(validBranchName)
                            .hashOnRef(invalidHash)
                            .submit())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(".hashOnRef: " + HASH_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        api.getCommitLog().refName(validBranchName).untilHash(invalidHash).submit())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("getCommitLog.params.startHash: " + HASH_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        api.getCommitLog().refName(validBranchName).hashOnRef(invalidHash).submit())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("getCommitLog.params.endHash: " + HASH_MESSAGE),
        () ->
            assertThatThrownBy(
                    () -> api.getEntries().refName(validBranchName).hashOnRef(invalidHash).submit())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("getEntries.params.hashOnRef: " + HASH_MESSAGE));
  }

  @ParameterizedTest
  @CsvSource({
    "" + COMMA_VALID_HASH_1,
    "abc'" + COMMA_VALID_HASH_1,
    ".foo" + COMMA_VALID_HASH_2,
    "abc'def'..'blah" + COMMA_VALID_HASH_2,
    "abc'de..blah" + COMMA_VALID_HASH_3,
    "abc'de@{blah" + COMMA_VALID_HASH_3
  })
  void invalidTags(String invalidTagNameIn, String validHash) {
    Assumptions.assumeThat(httpClient).isNotNull();
    // CsvSource maps an empty string as null
    String invalidTagName = invalidTagNameIn != null ? invalidTagNameIn : "";

    String validBranchName = "hello";
    // Need the string-ified JSON representation of `Tag` here, because `Tag` itself performs
    // validation.
    String tag =
        "{\"type\": \"TAG\", \"name\": \""
            + invalidTagName
            + "\", \"hash\": \""
            + validHash
            + "\"}";
    String branch =
        "{\"type\": \"BRANCH\", \"name\": \""
            + invalidTagName
            + "\", \"hash\": \""
            + validHash
            + "\"}";
    String different =
        "{\"type\": \"FOOBAR\", \"name\": \""
            + invalidTagName
            + "\", \"hash\": \""
            + validHash
            + "\"}";
    assertAll(
        () ->
            assertThatThrownBy(
                    () ->
                        unwrap(
                            () ->
                                httpClient
                                    .newRequest()
                                    .path("trees/tag/{tagName}")
                                    .resolveTemplate("tagName", validBranchName)
                                    .queryParam("expectedHash", validHash)
                                    .put(null)))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessage("Bad Request (HTTP/400): assignTag.assignTo: must not be null"),
        () ->
            assertThatThrownBy(
                    () ->
                        unwrap(
                            () ->
                                httpClient
                                    .newRequest()
                                    .path("trees/tag/{tagName}")
                                    .resolveTemplate("tagName", validBranchName)
                                    .queryParam("expectedHash", validHash)
                                    .put(tag)))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageStartingWith(
                    "Bad Request (HTTP/400): Cannot construct instance of "
                        + "`org.projectnessie.model.ImmutableTag`, problem: "
                        + REF_NAME_MESSAGE
                        + " - but was: "
                        + invalidTagName
                        + "\n"),
        () ->
            assertThatThrownBy(
                    () ->
                        unwrap(
                            () ->
                                httpClient
                                    .newRequest()
                                    .path("trees/tag/{tagName}")
                                    .resolveTemplate("tagName", validBranchName)
                                    .queryParam("expectedHash", validHash)
                                    .put(branch)))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageStartingWith("Bad Request (HTTP/400): Cannot construct instance of ")
                .hasMessageContaining(REF_NAME_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        unwrap(
                            () ->
                                httpClient
                                    .newRequest()
                                    .path("trees/tag/{tagName}")
                                    .resolveTemplate("tagName", validBranchName)
                                    .queryParam("expectedHash", validHash)
                                    .put(different)))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageStartingWith(
                    "Bad Request (HTTP/400): Could not resolve type id 'FOOBAR' as a subtype of "
                        + "`org.projectnessie.model.Reference`: known type ids = []\n"));
  }

  @Test
  public void testInvalidNamedRefs() {
    ContentsKey key = ContentsKey.of("x");
    String invalidRef = "1234567890123456";

    assertThatThrownBy(() -> api.getCommitLog().refName(invalidRef).submit())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageContaining("Bad Request (HTTP/400):")
        .hasMessageContaining("getCommitLog.ref: " + REF_NAME_MESSAGE);

    assertThatThrownBy(() -> api.getEntries().refName(invalidRef).submit())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageContaining("Bad Request (HTTP/400):")
        .hasMessageContaining("getEntries.refName: " + REF_NAME_MESSAGE);

    assertThatThrownBy(() -> api.getContents().key(key).refName(invalidRef).submit())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageContaining("Bad Request (HTTP/400):")
        .hasMessageContaining(".ref: " + REF_NAME_MESSAGE);

    assertThatThrownBy(() -> api.getContents().refName(invalidRef).key(key).submit())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageContaining("Bad Request (HTTP/400):")
        .hasMessageContaining(".ref: " + REF_NAME_MESSAGE);
  }

  @Test
  public void testValidHashesOnValidNamedRefs()
      throws NessieNotFoundException, NessieConflictException {
    Reference main = api.getReference().refName("main").submit();
    Branch b = Branch.of("testValidHashesOnValidNamedRefs", main.getHash());
    Reference branch = api.createReference().sourceRefName(main.getName()).reference(b).submit();
    assertThat(branch).isEqualTo(b);

    int commits = 10;

    String currentHash = main.getHash();
    createCommits(branch, 1, commits, currentHash);
    LogResponse entireLog = api.getCommitLog().refName(branch.getName()).submit();
    assertThat(entireLog).isNotNull();
    assertThat(entireLog.getOperations()).hasSize(commits);

    EntriesResponse allEntries = api.getEntries().refName(branch.getName()).submit();
    assertThat(allEntries).isNotNull();
    assertThat(allEntries.getEntries()).hasSize(commits);

    List<ContentsKey> keys = new ArrayList<>();
    IntStream.range(0, commits).forEach(i -> keys.add(ContentsKey.of("table" + i)));

    // TODO: check where hashOnRef is set
    Map<ContentsKey, Contents> allContents =
        api.getContents().keys(keys).refName(branch.getName()).submit();

    for (int i = 0; i < commits; i++) {
      String hash = entireLog.getOperations().get(i).getHash();
      LogResponse log = api.getCommitLog().refName(branch.getName()).hashOnRef(hash).submit();
      assertThat(log).isNotNull();
      assertThat(log.getOperations()).hasSize(commits - i);
      assertThat(ImmutableList.copyOf(entireLog.getOperations()).subList(i, commits))
          .containsExactlyElementsOf(log.getOperations());

      EntriesResponse entries = api.getEntries().refName(branch.getName()).hashOnRef(hash).submit();
      assertThat(entries).isNotNull();
      assertThat(entries.getEntries()).hasSize(commits - i);

      int idx = commits - 1 - i;
      ContentsKey key = ContentsKey.of("table" + idx);
      Contents c =
          api.getContents().key(key).refName(branch.getName()).hashOnRef(hash).submit().get(key);
      assertThat(c).isNotNull().isEqualTo(allContents.get(key));
    }
  }

  @Test
  public void testUnknownHashesOnValidNamedRefs()
      throws NessieNotFoundException, NessieConflictException {
    Reference main = api.getReference().refName("main").submit();
    Branch b = Branch.of("testUnknownHashesOnValidNamedRefs", main.getHash());
    Reference branch = api.createReference().sourceRefName(main.getName()).reference(b).submit();
    assertThat(branch).isEqualTo(b);
    String invalidHash = "1234567890123456";

    int commits = 10;

    String currentHash = main.getHash();
    createCommits(branch, 1, commits, currentHash);
    assertThatThrownBy(
            () -> api.getCommitLog().refName(branch.getName()).hashOnRef(invalidHash).submit())
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessageContaining(
            String.format(
                "Could not find commit '%s' in reference '%s'.", invalidHash, b.getName()));

    assertThatThrownBy(
            () -> api.getEntries().refName(branch.getName()).hashOnRef(invalidHash).submit())
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessageContaining(
            String.format(
                "Could not find commit '%s' in reference '%s'.", invalidHash, b.getName()));

    assertThatThrownBy(
            () ->
                api.getContents()
                    .key(ContentsKey.of("table0"))
                    .refName(branch.getName())
                    .hashOnRef(invalidHash)
                    .submit())
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessageContaining(
            String.format(
                "Could not find commit '%s' in reference '%s'.", invalidHash, b.getName()));

    assertThatThrownBy(
            () ->
                api.getContents()
                    .key(ContentsKey.of("table0"))
                    .refName(branch.getName())
                    .hashOnRef(invalidHash)
                    .submit())
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessageContaining(
            String.format(
                "Could not find commit '%s' in reference '%s'.", invalidHash, b.getName()));
  }

  void unwrap(Executable exec) throws Throwable {
    try {
      exec.execute();
    } catch (Throwable targetException) {
      if (targetException instanceof HttpClientException) {
        if (targetException.getCause() instanceof NessieNotFoundException
            || targetException.getCause() instanceof NessieConflictException) {
          throw targetException.getCause();
        }
      }

      throw targetException;
    }
  }
}
